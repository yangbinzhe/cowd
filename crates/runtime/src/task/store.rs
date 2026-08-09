//! Runtime-owned Task aggregate, persistence contract, and evidence outbox.

use std::{
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

pub use harness_contract::task::{
    TaskAggregate, TaskCommandOutcome, TaskCommandReceipt, TaskExecutionPolicy, TaskGraphRef,
    TaskKind, TaskMissionAssignment, TaskOrigin, TaskPhase, TaskPhaseArtifact, TaskPhaseStatus,
    TaskPhaseTerminalReceipt, TaskSpec, TaskStatus, TaskTurnBinding, TaskTurnRole,
};
use harness_contract::{
    mission::{
        MissionOrganizationDecision, MissionOrganizationStatus, TaskMissionAssignmentCommand,
        TaskMissionAssignmentPreview, TaskMissionAssignmentPreviewItem,
        TaskMissionAssignmentReceipt,
    },
    reality::EvidenceRef,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskMissionAssignmentOutboxRecord {
    pub operation_id: String,
    pub receipt: TaskMissionAssignmentReceipt,
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

    fn for_graphs(&self, graph_ids: &[String]) -> Result<Vec<TaskAggregate>, String> {
        let graph_ids = graph_ids
            .iter()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>();
        Ok(self
            .list()?
            .into_iter()
            .filter(|task| {
                task.graph_refs
                    .iter()
                    .any(|reference| graph_ids.contains(reference.graph_id.as_str()))
            })
            .collect())
    }

    fn get(&self, task_id: &str) -> Result<Option<TaskAggregate>, String> {
        Ok(self
            .list()?
            .into_iter()
            .find(|task| task.task_id == task_id))
    }

    fn bind_turn(&self, binding: &TaskTurnBinding) -> Result<TaskTurnBinding, String>;

    fn create_with_origin_binding(
        &self,
        aggregate: &TaskAggregate,
        mutation: &TaskMutation,
        binding: &TaskTurnBinding,
    ) -> Result<(TaskMutationResult, TaskTurnBinding), String>;

    fn bindings_for_task(&self, task_id: &str) -> Result<Vec<TaskTurnBinding>, String>;

    fn bindings_for_turn(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> Result<Vec<TaskTurnBinding>, String>;

    fn list_bindings(&self) -> Result<Vec<TaskTurnBinding>, String> {
        let mut bindings = Vec::new();
        for task in self.list()? {
            bindings.extend(self.bindings_for_task(&task.task_id)?);
        }
        Ok(bindings)
    }

    fn open_root_candidates(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<TaskAggregate>, String> {
        let mut tasks = std::collections::BTreeMap::new();
        for task in self.list()? {
            if !crate::task::is_open_root(&task) {
                continue;
            }
            if self
                .bindings_for_task(&task.task_id)?
                .iter()
                .any(|binding| binding.session_id == session_id)
            {
                tasks.insert(task.updated_at_ms, task);
            }
        }
        Ok(tasks.into_values().rev().take(limit).collect())
    }

    /// Return a bounded set of recent Root Tasks eligible for asynchronous
    /// Mission organization. Persistent backends override this method so the
    /// Runtime does not materialize the complete Task table.
    fn organization_candidates(&self, limit: usize) -> Result<Vec<TaskAggregate>, String> {
        let mut tasks = self
            .list()?
            .into_iter()
            .filter(crate::task::is_organization_candidate)
            .collect::<Vec<_>>();
        tasks.sort_by(|left, right| {
            right
                .updated_at_ms
                .cmp(&left.updated_at_ms)
                .then_with(|| left.task_id.cmp(&right.task_id))
        });
        tasks.truncate(limit);
        Ok(tasks)
    }

    /// Return a bounded set of eligible Root Tasks that do not yet own a
    /// durable organization decision. Persistent backends implement this as
    /// an indexed anti-join so worker recovery never requires a table scan.
    fn unorganized_candidates(&self, limit: usize) -> Result<Vec<TaskAggregate>, String> {
        let decided = self
            .organization_decisions(None, usize::MAX)?
            .into_iter()
            .filter_map(|decision| decision.canonical_root_task_id().map(str::to_string))
            .collect::<std::collections::BTreeSet<_>>();
        Ok(self
            .organization_candidates(limit.saturating_add(decided.len()))?
            .into_iter()
            .filter(|task| !decided.contains(&task.task_id))
            .take(limit)
            .collect())
    }

    fn assign_mission_batch(
        &self,
        command: &TaskMissionAssignmentCommand,
    ) -> Result<TaskMissionAssignmentReceipt, String>;

    fn assignment_receipt(
        &self,
        operation_id: &str,
    ) -> Result<Option<TaskMissionAssignmentReceipt>, String>;

    fn save_organization_decision(
        &self,
        decision: &MissionOrganizationDecision,
        expected_revision: Option<u64>,
    ) -> Result<MissionOrganizationDecision, String>;

    fn organization_decisions(
        &self,
        status: Option<MissionOrganizationStatus>,
        limit: usize,
    ) -> Result<Vec<MissionOrganizationDecision>, String>;

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

    fn list_assignment_outbox(&self) -> Result<Vec<TaskMissionAssignmentOutboxRecord>, String>;

    fn mark_outbox_projected(&self, outbox_id: &str, projected_at_ms: u64) -> Result<(), String>;

    fn export_migration_snapshot(&self) -> Result<TaskStoreSnapshot, String> {
        let snapshot = TaskStoreSnapshot {
            tasks: self.list()?,
            bindings: self.list_bindings()?,
            outbox: self.list_outbox()?,
            assignment_outbox: self.list_assignment_outbox()?,
            organization_decisions: self.organization_decisions(None, usize::MAX)?,
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
    pub bindings: Vec<TaskTurnBinding>,
    pub outbox: Vec<TaskEvidenceOutboxRecord>,
    pub assignment_outbox: Vec<TaskMissionAssignmentOutboxRecord>,
    pub organization_decisions: Vec<MissionOrganizationDecision>,
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
        harness_contract::task::validate_turn_bindings(&self.bindings)?;
        for binding in &self.bindings {
            if !task_ids.contains(&binding.task_id) {
                return Err(format!(
                    "task binding `{}` has no aggregate",
                    binding.binding_id
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
        let mut operation_ids = std::collections::BTreeSet::new();
        for record in &self.assignment_outbox {
            if !operation_ids.insert(record.operation_id.as_str())
                || record.operation_id != record.receipt.operation_id
            {
                return Err(
                    "task assignment migration snapshot contains an invalid duplicate operation"
                        .to_string(),
                );
            }
        }
        let mut decision_ids = std::collections::BTreeSet::new();
        for decision in &self.organization_decisions {
            if !decision_ids.insert(decision.decision_id.as_str()) || decision.revision == 0 {
                return Err(
                    "mission organization snapshot contains an invalid decision".to_string()
                );
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
        canonical.bindings.sort_by(|left, right| {
            left.bound_at_ms
                .cmp(&right.bound_at_ms)
                .then_with(|| left.binding_id.cmp(&right.binding_id))
        });
        canonical
            .outbox
            .sort_by(|left, right| left.outbox_id.cmp(&right.outbox_id));
        canonical
            .assignment_outbox
            .sort_by(|left, right| left.operation_id.cmp(&right.operation_id));
        canonical
            .organization_decisions
            .sort_by(|left, right| left.decision_id.cmp(&right.decision_id));
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

    fn organization_candidates(&self, limit: usize) -> Result<Vec<TaskAggregate>, String> {
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let mut statement = connection
            .prepare(
                "SELECT record_json FROM tasks
                  WHERE status IN ('pending','running','reviewing','blocked')
                    AND json_extract(record_json,'$.kind')='root'
                    AND json_extract(record_json,'$.origin')!='system'
                    AND json_extract(record_json,'$.mission_assignment')!='explicit_locked'
                  ORDER BY updated_at_ms DESC,id ASC LIMIT ?1",
            )
            .map_err(|error| error.to_string())?;
        let rows = statement
            .query_map(params![i64::try_from(limit).unwrap_or(i64::MAX)], |row| {
                row.get::<_, String>(0)
            })
            .map_err(|error| error.to_string())?;
        rows.map(|row| {
            serde_json::from_str(&row.map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())
        })
        .collect()
    }

    fn unorganized_candidates(&self, limit: usize) -> Result<Vec<TaskAggregate>, String> {
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let mut statement = connection
            .prepare(
                "SELECT task.record_json FROM tasks AS task
                  WHERE task.status IN ('pending','running','reviewing','blocked')
                    AND json_extract(task.record_json,'$.kind')='root'
                    AND json_extract(task.record_json,'$.origin')!='system'
                    AND json_extract(task.record_json,'$.mission_assignment')!='explicit_locked'
                    AND NOT EXISTS (
                        SELECT 1 FROM mission_organization_decisions AS decision
                         WHERE decision.decision_id='mission-organization:' || task.id
                    )
                  ORDER BY task.updated_at_ms DESC,task.id ASC LIMIT ?1",
            )
            .map_err(|error| error.to_string())?;
        let rows = statement
            .query_map(params![i64::try_from(limit).unwrap_or(i64::MAX)], |row| {
                row.get::<_, String>(0)
            })
            .map_err(|error| error.to_string())?;
        rows.map(|row| {
            serde_json::from_str(&row.map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())
        })
        .collect()
    }

    fn open_root_candidates(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<TaskAggregate>, String> {
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let mut statement = connection
            .prepare(
                "SELECT DISTINCT tasks.record_json,tasks.updated_at_ms,tasks.id
                   FROM task_turn_bindings
                   JOIN tasks ON tasks.id=task_turn_bindings.task_id
                  WHERE task_turn_bindings.session_id=?1
                    AND tasks.status IN ('pending','running','reviewing','blocked')
                    AND json_extract(tasks.record_json,'$.kind')='root'
                  ORDER BY tasks.updated_at_ms DESC,tasks.id ASC LIMIT ?2",
            )
            .map_err(|error| error.to_string())?;
        let rows = statement
            .query_map(
                params![session_id, i64::try_from(limit).unwrap_or(i64::MAX)],
                |row| row.get::<_, String>(0),
            )
            .map_err(|error| error.to_string())?;
        rows.map(|row| {
            serde_json::from_str(&row.map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())
        })
        .collect()
    }

    fn for_graphs(&self, graph_ids: &[String]) -> Result<Vec<TaskAggregate>, String> {
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let mut tasks = std::collections::BTreeMap::new();
        let mut statement = connection
            .prepare(
                "SELECT tasks.record_json
                   FROM task_graph_refs
                   JOIN tasks ON tasks.id = task_graph_refs.task_id
                  WHERE task_graph_refs.graph_id = ?1",
            )
            .map_err(|error| error.to_string())?;
        for graph_id in graph_ids {
            let rows = statement
                .query_map(params![graph_id], |row| row.get::<_, String>(0))
                .map_err(|error| error.to_string())?;
            for row in rows {
                let task: TaskAggregate =
                    serde_json::from_str(&row.map_err(|error| error.to_string())?)
                        .map_err(|error| error.to_string())?;
                tasks.insert(task.task_id.clone(), task);
            }
        }
        Ok(tasks.into_values().collect())
    }

    fn bind_turn(&self, binding: &TaskTurnBinding) -> Result<TaskTurnBinding, String> {
        crate::task::validate_binding(binding)?;
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let record_json = serde_json::to_string(binding).map_err(|error| error.to_string())?;
        connection
            .execute(
                "INSERT INTO task_turn_bindings(
                     binding_id,task_id,session_id,turn_id,role,input_id,bound_at_ms,record_json
                 ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)
                 ON CONFLICT(task_id,session_id,turn_id) DO NOTHING",
                params![
                    binding.binding_id,
                    binding.task_id,
                    binding.session_id,
                    binding.turn_id,
                    task_turn_role_name(binding.role),
                    binding.input_id,
                    to_i64(binding.bound_at_ms, "bound_at_ms")?,
                    record_json,
                ],
            )
            .map_err(|error| error.to_string())?;
        let stored = connection
            .query_row(
                "SELECT record_json FROM task_turn_bindings
                  WHERE task_id=?1 AND session_id=?2 AND turn_id=?3",
                params![binding.task_id, binding.session_id, binding.turn_id],
                |row| row.get::<_, String>(0),
            )
            .map_err(|error| error.to_string())?;
        let stored: TaskTurnBinding =
            serde_json::from_str(&stored).map_err(|error| error.to_string())?;
        if stored != *binding {
            return Err(format!(
                "turn `{}` is already bound to task `{}` with different data",
                binding.turn_id, binding.task_id
            ));
        }
        Ok(stored)
    }

    fn create_with_origin_binding(
        &self,
        aggregate: &TaskAggregate,
        mutation: &TaskMutation,
        binding: &TaskTurnBinding,
    ) -> Result<(TaskMutationResult, TaskTurnBinding), String> {
        validate_task_aggregate_for_backend(aggregate)?;
        crate::task::validate_binding(binding)?;
        let mut connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        let current = transaction
            .query_row(
                "SELECT record_json FROM tasks WHERE id=?1",
                params![aggregate.task_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| error.to_string())?
            .map(|raw| {
                serde_json::from_str::<TaskAggregate>(&raw).map_err(|error| error.to_string())
            })
            .transpose()?;
        let (stored_task, outbox) = if let Some(current) = current {
            if !same_immutable_task_creation(&current, aggregate) {
                return Err(format!(
                    "task id `{}` is already bound to different immutable creation data",
                    aggregate.task_id
                ));
            }
            let raw = transaction
                .query_row(
                    "SELECT record_json FROM task_evidence_outbox
                      WHERE task_id=?1 AND revision=?2",
                    params![current.task_id, to_i64(current.revision, "revision")?],
                    |row| row.get::<_, String>(0),
                )
                .map_err(|error| error.to_string())?;
            let outbox = serde_json::from_str(&raw).map_err(|error| error.to_string())?;
            (current, outbox)
        } else {
            let outbox = validate_backend_mutation(&aggregate.task_id, None, aggregate, mutation)?
                .ok_or_else(|| "Task creation requires an evidence outbox".to_string())?;
            transaction
                .execute(
                    "INSERT INTO tasks(id,status,created_at_ms,updated_at_ms,record_json)
                     VALUES(?1,?2,?3,?4,?5)",
                    params![
                        aggregate.task_id,
                        aggregate.status.as_str(),
                        to_i64(aggregate.created_at_ms, "created_at_ms")?,
                        to_i64(aggregate.updated_at_ms, "updated_at_ms")?,
                        serde_json::to_string(aggregate).map_err(|error| error.to_string())?,
                    ],
                )
                .map_err(|error| error.to_string())?;
            sync_task_graph_refs_sqlite(&transaction, aggregate)?;
            transaction
                .execute(
                    "INSERT INTO task_evidence_outbox(
                        outbox_id,task_id,revision,event_kind,created_at_ms,record_json
                     ) VALUES(?1,?2,?3,?4,?5,?6)",
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
            (aggregate.clone(), outbox)
        };
        transaction
            .execute(
                "INSERT INTO task_turn_bindings(
                    binding_id,task_id,session_id,turn_id,role,input_id,bound_at_ms,record_json
                 ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)
                 ON CONFLICT(task_id,session_id,turn_id) DO NOTHING",
                params![
                    binding.binding_id,
                    binding.task_id,
                    binding.session_id,
                    binding.turn_id,
                    task_turn_role_name(binding.role),
                    binding.input_id,
                    to_i64(binding.bound_at_ms, "bound_at_ms")?,
                    serde_json::to_string(binding).map_err(|error| error.to_string())?,
                ],
            )
            .map_err(|error| error.to_string())?;
        let raw = transaction
            .query_row(
                "SELECT record_json FROM task_turn_bindings
                  WHERE task_id=?1 AND session_id=?2 AND turn_id=?3",
                params![binding.task_id, binding.session_id, binding.turn_id],
                |row| row.get::<_, String>(0),
            )
            .map_err(|error| error.to_string())?;
        let stored_binding = serde_json::from_str(&raw).map_err(|error| error.to_string())?;
        if stored_binding != *binding {
            return Err(format!(
                "turn `{}` has a conflicting origin Task binding",
                binding.turn_id
            ));
        }
        transaction.commit().map_err(|error| error.to_string())?;
        Ok((
            TaskMutationResult::from_backend_commit(stored_task, mutation, Some(outbox)),
            stored_binding,
        ))
    }

    fn bindings_for_task(&self, task_id: &str) -> Result<Vec<TaskTurnBinding>, String> {
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        load_bindings_sqlite(
            &connection,
            "SELECT record_json FROM task_turn_bindings
              WHERE task_id=?1 ORDER BY bound_at_ms ASC,binding_id ASC",
            params![task_id],
        )
    }

    fn bindings_for_turn(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> Result<Vec<TaskTurnBinding>, String> {
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        load_bindings_sqlite(
            &connection,
            "SELECT record_json FROM task_turn_bindings
              WHERE session_id=?1 AND turn_id=?2
              ORDER BY CASE role WHEN 'primary' THEN 0 ELSE 1 END,bound_at_ms ASC,binding_id ASC",
            params![session_id, turn_id],
        )
    }

    fn assign_mission_batch(
        &self,
        command: &TaskMissionAssignmentCommand,
    ) -> Result<TaskMissionAssignmentReceipt, String> {
        validate_assignment_command(command)?;
        let mut connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        if let Some(raw) = transaction
            .query_row(
                "SELECT record_json FROM task_mission_assignment_outbox WHERE operation_id=?1",
                params![command.operation_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| error.to_string())?
        {
            let record: TaskMissionAssignmentOutboxRecord =
                serde_json::from_str(&raw).map_err(|error| error.to_string())?;
            validate_assignment_replay(command, &record.receipt)?;
            return Ok(record.receipt);
        }
        let mut updated = Vec::with_capacity(command.task_ids.len());
        let applied_at_ms = now_ms();
        for task_id in &command.task_ids {
            let raw = transaction
                .query_row(
                    "SELECT record_json FROM tasks WHERE id=?1",
                    params![task_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(|error| error.to_string())?
                .ok_or_else(|| format!("task `{task_id}` not found"))?;
            let mut task: TaskAggregate =
                serde_json::from_str(&raw).map_err(|error| error.to_string())?;
            let expected = command
                .expected_task_revisions
                .get(task_id)
                .copied()
                .ok_or_else(|| format!("task `{task_id}` has no expected revision"))?;
            if task.revision != expected {
                return Err(format!(
                    "task `{task_id}` revision conflict: expected {expected}, actual {}",
                    task.revision
                ));
            }
            if task.mission_assignment == TaskMissionAssignment::ExplicitLocked
                && command.assignment != TaskMissionAssignment::ExplicitLocked
            {
                return Err(format!("task `{task_id}` has an explicit Mission lock"));
            }
            task.mission_id.clone_from(&command.target_mission_id);
            task.mission_assignment = command.assignment;
            task.mission_assignment_revision = task.mission_assignment_revision.saturating_add(1);
            task.mission_assigned_by.clone_from(&command.actor);
            task.mission_assignment_evidence_refs = command.evidence_refs.clone();
            task.revision = task.revision.saturating_add(1);
            task.updated_at_ms = applied_at_ms;
            validate_task_aggregate_for_backend(&task)?;
            updated.push(task);
        }
        let selected = updated
            .iter()
            .map(|task| task.task_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        for task in &updated {
            if task.kind == TaskKind::Delegated && !selected.contains(task.root_task_id.as_str()) {
                let root_raw = transaction
                    .query_row(
                        "SELECT record_json FROM tasks WHERE id=?1",
                        params![task.root_task_id],
                        |row| row.get::<_, String>(0),
                    )
                    .map_err(|error| error.to_string())?;
                let root: TaskAggregate =
                    serde_json::from_str(&root_raw).map_err(|error| error.to_string())?;
                if root.mission_id != command.target_mission_id {
                    return Err(format!(
                        "delegated task `{}` cannot leave root task `{}` in another Mission",
                        task.task_id, task.root_task_id
                    ));
                }
            }
        }
        let mut task_revisions = std::collections::BTreeMap::new();
        for task in &updated {
            transaction
                .execute(
                    "UPDATE tasks SET status=?2,updated_at_ms=?3,record_json=?4 WHERE id=?1",
                    params![
                        task.task_id,
                        task.status.as_str(),
                        to_i64(task.updated_at_ms, "updated_at_ms")?,
                        serde_json::to_string(task).map_err(|error| error.to_string())?,
                    ],
                )
                .map_err(|error| error.to_string())?;
            let outbox = TaskEvidenceOutboxRecord {
                outbox_id: format!("task-outbox:{}:{}", task.task_id, task.revision),
                task_id: task.task_id.clone(),
                revision: task.revision,
                event_kind: "task.mission_assigned".to_string(),
                status: task.status,
                evidence_refs: command.evidence_refs.clone(),
                created_at_ms: applied_at_ms,
                projected_at_ms: None,
            };
            transaction
                .execute(
                    "INSERT INTO task_evidence_outbox(
                        outbox_id,task_id,revision,event_kind,created_at_ms,record_json
                     ) VALUES(?1,?2,?3,?4,?5,?6)",
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
            task_revisions.insert(task.task_id.clone(), task.revision);
        }
        let receipt = TaskMissionAssignmentReceipt {
            operation_id: command.operation_id.clone(),
            target_mission_id: command.target_mission_id.clone(),
            task_revisions,
            assignment: command.assignment,
            applied_at_ms,
            evidence_refs: command.evidence_refs.clone(),
        };
        let record = TaskMissionAssignmentOutboxRecord {
            operation_id: command.operation_id.clone(),
            receipt: receipt.clone(),
            created_at_ms: applied_at_ms,
            projected_at_ms: None,
        };
        transaction
            .execute(
                "INSERT INTO task_mission_assignment_outbox(
                    operation_id,created_at_ms,record_json
                 ) VALUES(?1,?2,?3)",
                params![
                    record.operation_id,
                    to_i64(record.created_at_ms, "created_at_ms")?,
                    serde_json::to_string(&record).map_err(|error| error.to_string())?,
                ],
            )
            .map_err(|error| error.to_string())?;
        transaction.commit().map_err(|error| error.to_string())?;
        Ok(receipt)
    }

    fn assignment_receipt(
        &self,
        operation_id: &str,
    ) -> Result<Option<TaskMissionAssignmentReceipt>, String> {
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        connection
            .query_row(
                "SELECT record_json FROM task_mission_assignment_outbox WHERE operation_id=?1",
                params![operation_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| error.to_string())?
            .map(|raw| {
                serde_json::from_str::<TaskMissionAssignmentOutboxRecord>(&raw)
                    .map(|record| record.receipt)
                    .map_err(|error| error.to_string())
            })
            .transpose()
    }

    fn save_organization_decision(
        &self,
        decision: &MissionOrganizationDecision,
        expected_revision: Option<u64>,
    ) -> Result<MissionOrganizationDecision, String> {
        validate_organization_decision(decision)?;
        let mut connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        let existing = transaction
            .query_row(
                "SELECT record_json FROM mission_organization_decisions WHERE decision_id=?1",
                params![decision.decision_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| error.to_string())?
            .map(|raw| serde_json::from_str::<MissionOrganizationDecision>(&raw))
            .transpose()
            .map_err(|error| error.to_string())?;
        match (existing.as_ref(), expected_revision) {
            (None, None) => {}
            (Some(existing), Some(expected)) if existing.revision == expected => {}
            (Some(existing), None)
                if existing.decision_id == decision.decision_id
                    && existing.workspace_id == decision.workspace_id
                    && existing.canonical_root_task_id() == decision.canonical_root_task_id() =>
            {
                return Ok(existing.clone());
            }
            (Some(existing), _) => {
                return Err(format!(
                    "organization decision `{}` revision conflict at {}",
                    decision.decision_id, existing.revision
                ));
            }
            (None, Some(_)) => {
                return Err(format!(
                    "organization decision `{}` does not exist",
                    decision.decision_id
                ));
            }
        }
        transaction
            .execute(
                "INSERT INTO mission_organization_decisions(
                    decision_id,status,next_attempt_at_ms,created_at_ms,updated_at_ms,record_json
                 ) VALUES(?1,?2,?3,?4,?5,?6)
                 ON CONFLICT(decision_id) DO UPDATE SET
                    status=excluded.status,next_attempt_at_ms=excluded.next_attempt_at_ms,
                    updated_at_ms=excluded.updated_at_ms,record_json=excluded.record_json",
                params![
                    decision.decision_id,
                    mission_organization_status_name(decision.status),
                    to_i64(decision.next_attempt_at_ms, "next_attempt_at_ms")?,
                    to_i64(decision.created_at_ms, "created_at_ms")?,
                    to_i64(decision.updated_at_ms, "updated_at_ms")?,
                    serde_json::to_string(decision).map_err(|error| error.to_string())?,
                ],
            )
            .map_err(|error| error.to_string())?;
        transaction.commit().map_err(|error| error.to_string())?;
        Ok(decision.clone())
    }

    fn organization_decisions(
        &self,
        status: Option<MissionOrganizationStatus>,
        limit: usize,
    ) -> Result<Vec<MissionOrganizationDecision>, String> {
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let sql = if status.is_some() {
            "SELECT record_json FROM mission_organization_decisions
              WHERE status=?1 ORDER BY created_at_ms ASC,decision_id ASC LIMIT ?2"
        } else {
            "SELECT record_json FROM mission_organization_decisions
              ORDER BY created_at_ms ASC,decision_id ASC LIMIT ?1"
        };
        let mut statement = connection.prepare(sql).map_err(|error| error.to_string())?;
        let rows = if let Some(status) = status {
            statement
                .query_map(
                    params![
                        mission_organization_status_name(status),
                        i64::try_from(limit).unwrap_or(i64::MAX)
                    ],
                    |row| row.get::<_, String>(0),
                )
                .map_err(|error| error.to_string())?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.to_string())?
        } else {
            statement
                .query_map(params![i64::try_from(limit).unwrap_or(i64::MAX)], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(|error| error.to_string())?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.to_string())?
        };
        rows.into_iter()
            .map(|raw| serde_json::from_str(&raw).map_err(|error| error.to_string()))
            .collect()
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
        sync_task_graph_refs_sqlite(&transaction, &next)?;
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

    fn list_assignment_outbox(&self) -> Result<Vec<TaskMissionAssignmentOutboxRecord>, String> {
        let connection = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let mut statement = connection
            .prepare(
                "SELECT record_json FROM task_mission_assignment_outbox
                 ORDER BY created_at_ms ASC, operation_id ASC",
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
        let existing_bindings: i64 = transaction
            .query_row("SELECT COUNT(*) FROM task_turn_bindings", [], |row| {
                row.get(0)
            })
            .map_err(|error| error.to_string())?;
        let existing_assignments: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM task_mission_assignment_outbox",
                [],
                |row| row.get(0),
            )
            .map_err(|error| error.to_string())?;
        let existing_decisions: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM mission_organization_decisions",
                [],
                |row| row.get(0),
            )
            .map_err(|error| error.to_string())?;
        if existing_tasks != 0
            || existing_bindings != 0
            || existing_outbox != 0
            || existing_assignments != 0
            || existing_decisions != 0
        {
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
            sync_task_graph_refs_sqlite(&transaction, task)?;
        }
        for binding in &snapshot.bindings {
            transaction
                .execute(
                    "INSERT INTO task_turn_bindings(
                         binding_id,task_id,session_id,turn_id,role,input_id,bound_at_ms,record_json
                     ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                    params![
                        binding.binding_id,
                        binding.task_id,
                        binding.session_id,
                        binding.turn_id,
                        task_turn_role_name(binding.role),
                        binding.input_id,
                        to_i64(binding.bound_at_ms, "bound_at_ms")?,
                        serde_json::to_string(binding).map_err(|error| error.to_string())?,
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
        for record in &snapshot.assignment_outbox {
            transaction
                .execute(
                    "INSERT INTO task_mission_assignment_outbox(
                        operation_id,created_at_ms,projected_at_ms,record_json
                     ) VALUES(?1,?2,?3,?4)",
                    params![
                        record.operation_id,
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
        for decision in &snapshot.organization_decisions {
            transaction
                .execute(
                    "INSERT INTO mission_organization_decisions(
                        decision_id,status,next_attempt_at_ms,created_at_ms,updated_at_ms,record_json
                     ) VALUES(?1,?2,?3,?4,?5,?6)",
                    params![
                        decision.decision_id,
                        mission_organization_status_name(decision.status),
                        to_i64(decision.next_attempt_at_ms, "next_attempt_at_ms")?,
                        to_i64(decision.created_at_ms, "created_at_ms")?,
                        to_i64(decision.updated_at_ms, "updated_at_ms")?,
                        serde_json::to_string(decision).map_err(|error| error.to_string())?,
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

    pub fn for_graphs(&self, graph_ids: &[String]) -> Result<Vec<TaskAggregate>, String> {
        self.backend.for_graphs(graph_ids)
    }

    pub fn bind_turn(&self, binding: &TaskTurnBinding) -> Result<TaskTurnBinding, String> {
        self.backend.bind_turn(binding)
    }

    pub fn bindings_for_task(&self, task_id: &str) -> Result<Vec<TaskTurnBinding>, String> {
        self.backend.bindings_for_task(task_id)
    }

    pub fn bindings_for_turn(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> Result<Vec<TaskTurnBinding>, String> {
        self.backend.bindings_for_turn(session_id, turn_id)
    }

    pub fn open_root_candidates(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<TaskAggregate>, String> {
        self.backend.open_root_candidates(session_id, limit)
    }

    pub fn organization_candidates(&self, limit: usize) -> Result<Vec<TaskAggregate>, String> {
        self.backend.organization_candidates(limit)
    }

    pub fn unorganized_candidates(&self, limit: usize) -> Result<Vec<TaskAggregate>, String> {
        self.backend.unorganized_candidates(limit)
    }

    pub fn preview_mission_assignment(
        &self,
        mut command: TaskMissionAssignmentCommand,
    ) -> Result<(TaskMissionAssignmentCommand, TaskMissionAssignmentPreview), String> {
        validate_assignment_command(&command)?;
        let all = self.list()?;
        let requested = command
            .task_ids
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let selected_roots = all
            .iter()
            .filter(|task| requested.contains(&task.task_id) && task.kind == TaskKind::Root)
            .map(|task| task.task_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let mut expanded = requested;
        expanded.extend(
            all.iter()
                .filter(|task| selected_roots.contains(task.root_task_id.as_str()))
                .map(|task| task.task_id.clone()),
        );
        command.task_ids = expanded.into_iter().collect();
        let mut items = Vec::with_capacity(command.task_ids.len());
        command.expected_task_revisions.clear();
        for task_id in &command.task_ids {
            let task = all
                .iter()
                .find(|task| &task.task_id == task_id)
                .ok_or_else(|| format!("task `{task_id}` not found"))?;
            let allowed = task.mission_assignment != TaskMissionAssignment::ExplicitLocked
                || command.assignment == TaskMissionAssignment::ExplicitLocked;
            command
                .expected_task_revisions
                .insert(task.task_id.clone(), task.revision);
            items.push(TaskMissionAssignmentPreviewItem {
                task_id: task.task_id.clone(),
                current_mission_id: task.mission_id.clone(),
                target_mission_id: command.target_mission_id.clone(),
                current_revision: task.revision,
                allowed,
                reason: (!allowed)
                    .then(|| "explicitly locked Task requires an explicit assignment".to_string()),
            });
        }
        let preview = TaskMissionAssignmentPreview {
            operation_id: command.operation_id.clone(),
            target_mission_id: command.target_mission_id.clone(),
            confirm_required: items.len() > 1,
            items,
        };
        Ok((command, preview))
    }

    pub fn assign_mission_batch(
        &self,
        command: &TaskMissionAssignmentCommand,
    ) -> Result<TaskMissionAssignmentReceipt, String> {
        validate_assignment_command(command)?;
        self.backend.assign_mission_batch(command)
    }

    pub fn assignment_receipt(
        &self,
        operation_id: &str,
    ) -> Result<Option<TaskMissionAssignmentReceipt>, String> {
        self.backend.assignment_receipt(operation_id)
    }

    pub fn save_organization_decision(
        &self,
        decision: &MissionOrganizationDecision,
        expected_revision: Option<u64>,
    ) -> Result<MissionOrganizationDecision, String> {
        validate_organization_decision(decision)?;
        self.backend
            .save_organization_decision(decision, expected_revision)
    }

    pub fn organization_decisions(
        &self,
        status: Option<MissionOrganizationStatus>,
        limit: usize,
    ) -> Result<Vec<MissionOrganizationDecision>, String> {
        self.backend.organization_decisions(status, limit)
    }

    pub fn create(&self, command: TaskCreateCommand) -> Result<TaskMutationResult, String> {
        let (aggregate, mutation) = prepare_task_creation(command)?;
        let task_id = aggregate.task_id.clone();
        self.backend
            .mutate_task(&task_id, &mutation, &mut |current| {
                if let Some(existing) = current {
                    if same_immutable_task_creation(&existing, &aggregate) {
                        return Ok(existing);
                    }
                    return Err(format!(
                        "task id `{task_id}` is already bound to different immutable creation data"
                    ));
                }
                Ok(aggregate.clone())
            })
    }

    pub fn create_with_origin_binding(
        &self,
        command: TaskCreateCommand,
        binding: &TaskTurnBinding,
    ) -> Result<(TaskMutationResult, TaskTurnBinding), String> {
        let (aggregate, mutation) = prepare_task_creation(command)?;
        crate::task::validate_binding(binding)?;
        if binding.task_id != aggregate.task_id
            || binding.session_id != aggregate.origin_session_id
            || binding.turn_id != aggregate.origin_turn_id
        {
            return Err(
                "origin binding must match the Task id, origin Session and origin Turn".to_string(),
            );
        }
        self.backend
            .create_with_origin_binding(&aggregate, &mutation, binding)
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
                    if reference.revision >= graph_revision {
                        return Ok(());
                    }
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
    crate::task::validate_aggregate(task)
}

fn validate_assignment_command(command: &TaskMissionAssignmentCommand) -> Result<(), String> {
    for (field, value) in [
        ("operation_id", command.operation_id.as_str()),
        ("workspace_id", command.workspace_id.as_str()),
        ("target_mission_id", command.target_mission_id.as_str()),
        ("actor", command.actor.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(format!("task mission assignment requires {field}"));
        }
    }
    if command.task_ids.is_empty() {
        return Err("task mission assignment requires at least one Task".to_string());
    }
    let unique = command
        .task_ids
        .iter()
        .collect::<std::collections::BTreeSet<_>>();
    if unique.len() != command.task_ids.len() {
        return Err("task mission assignment contains duplicate Task ids".to_string());
    }
    if command.expected_task_revisions.len() != command.task_ids.len()
        || command
            .task_ids
            .iter()
            .any(|task_id| !command.expected_task_revisions.contains_key(task_id))
    {
        return Err(
            "task mission assignment requires an expected revision for every Task".to_string(),
        );
    }
    Ok(())
}

fn validate_assignment_replay(
    command: &TaskMissionAssignmentCommand,
    receipt: &TaskMissionAssignmentReceipt,
) -> Result<(), String> {
    let requested = command
        .task_ids
        .iter()
        .collect::<std::collections::BTreeSet<_>>();
    let committed = receipt
        .task_revisions
        .keys()
        .collect::<std::collections::BTreeSet<_>>();
    if receipt.operation_id != command.operation_id
        || receipt.target_mission_id != command.target_mission_id
        || receipt.assignment != command.assignment
        || requested != committed
    {
        return Err(format!(
            "task Mission assignment operation `{}` was reused with a different command",
            command.operation_id
        ));
    }
    Ok(())
}

fn validate_organization_decision(decision: &MissionOrganizationDecision) -> Result<(), String> {
    for (field, value) in [
        ("decision_id", decision.decision_id.as_str()),
        ("workspace_id", decision.workspace_id.as_str()),
        ("target_mission_id", decision.target_mission_id.as_str()),
        ("reason", decision.reason.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(format!("mission organization decision requires {field}"));
        }
    }
    if decision.canonical_root_task_id().is_none()
        || decision.affected_task_ids.is_empty()
        || decision.revision == 0
    {
        return Err(
            "mission organization decision requires Tasks and a positive revision".to_string(),
        );
    }
    Ok(())
}

fn mission_organization_status_name(status: MissionOrganizationStatus) -> &'static str {
    match status {
        MissionOrganizationStatus::Pending => "pending",
        MissionOrganizationStatus::Claimed => "claimed",
        MissionOrganizationStatus::Applied => "applied",
        MissionOrganizationStatus::Rejected => "rejected",
        MissionOrganizationStatus::Failed => "failed",
    }
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
        CREATE TABLE IF NOT EXISTS task_graph_refs (
            task_id TEXT NOT NULL,
            graph_id TEXT NOT NULL,
            graph_revision INTEGER NOT NULL,
            PRIMARY KEY(task_id, graph_id),
            FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_task_graph_refs_graph
            ON task_graph_refs(graph_id, task_id);
        CREATE TABLE IF NOT EXISTS task_turn_bindings (
            binding_id TEXT PRIMARY KEY NOT NULL,
            task_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            turn_id TEXT NOT NULL,
            role TEXT NOT NULL,
            input_id TEXT,
            bound_at_ms INTEGER NOT NULL,
            record_json TEXT NOT NULL,
            UNIQUE(task_id, session_id, turn_id),
            FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_task_turn_bindings_primary
            ON task_turn_bindings(session_id, turn_id) WHERE role='primary';
        CREATE INDEX IF NOT EXISTS idx_task_turn_bindings_turn
            ON task_turn_bindings(session_id, turn_id, bound_at_ms, binding_id);
        CREATE INDEX IF NOT EXISTS idx_task_turn_bindings_task
            ON task_turn_bindings(task_id, bound_at_ms, binding_id);
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
            ON task_evidence_outbox(projected_at_ms, created_at_ms, outbox_id);
        CREATE TABLE IF NOT EXISTS task_mission_assignment_outbox (
            operation_id TEXT PRIMARY KEY NOT NULL,
            created_at_ms INTEGER NOT NULL,
            projected_at_ms INTEGER,
            record_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_task_mission_assignment_outbox_pending
            ON task_mission_assignment_outbox(projected_at_ms, created_at_ms, operation_id);
        CREATE TABLE IF NOT EXISTS mission_organization_decisions (
            decision_id TEXT PRIMARY KEY NOT NULL,
            status TEXT NOT NULL,
            next_attempt_at_ms INTEGER NOT NULL,
            created_at_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL,
            record_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_mission_organization_decisions_claim
            ON mission_organization_decisions(status, next_attempt_at_ms, created_at_ms, decision_id);",
    )
    .map_err(|error| error.to_string())?;
    migrate_legacy_task_aggregates_sqlite(conn)?;
    let tasks = load_tasks_connection(conn)?;
    for task in tasks {
        sync_task_graph_refs_sqlite(conn, &task)?;
    }
    Ok(())
}

fn migrate_legacy_task_aggregates_sqlite(conn: &rusqlite::Connection) -> Result<(), String> {
    conn.execute_batch(
        "INSERT OR IGNORE INTO task_turn_bindings(
            binding_id,task_id,session_id,turn_id,role,input_id,bound_at_ms,record_json
         )
         SELECT 'task-turn:legacy:' || id || ':' || COALESCE(NULLIF(json_extract(record_json,'$.source_turn_id'),''), 'legacy-turn:' || id),
                id,
                COALESCE(NULLIF(json_extract(record_json,'$.source_session_id'),''), 'legacy-session:' || id),
                COALESCE(NULLIF(json_extract(record_json,'$.source_turn_id'),''), 'legacy-turn:' || id),
                CASE WHEN id=(
                    SELECT MIN(peer.id)
                      FROM tasks AS peer
                     WHERE COALESCE(NULLIF(json_extract(peer.record_json,'$.source_session_id'),''), 'legacy-session:' || peer.id)=
                           COALESCE(NULLIF(json_extract(tasks.record_json,'$.source_session_id'),''), 'legacy-session:' || tasks.id)
                       AND COALESCE(NULLIF(json_extract(peer.record_json,'$.source_turn_id'),''), 'legacy-turn:' || peer.id)=
                           COALESCE(NULLIF(json_extract(tasks.record_json,'$.source_turn_id'),''), 'legacy-turn:' || tasks.id)
                       AND json_type(peer.record_json,'$.kind') IS NULL
                ) THEN 'primary' ELSE 'additional' END,
                NULL,
                created_at_ms,
                json_object(
                    'binding_id','task-turn:legacy:' || id || ':' || COALESCE(NULLIF(json_extract(record_json,'$.source_turn_id'),''), 'legacy-turn:' || id),
                    'task_id',id,
                    'session_id',COALESCE(NULLIF(json_extract(record_json,'$.source_session_id'),''), 'legacy-session:' || id),
                    'turn_id',COALESCE(NULLIF(json_extract(record_json,'$.source_turn_id'),''), 'legacy-turn:' || id),
                    'role',CASE WHEN id=(
                        SELECT MIN(peer.id)
                          FROM tasks AS peer
                         WHERE COALESCE(NULLIF(json_extract(peer.record_json,'$.source_session_id'),''), 'legacy-session:' || peer.id)=
                               COALESCE(NULLIF(json_extract(tasks.record_json,'$.source_session_id'),''), 'legacy-session:' || tasks.id)
                           AND COALESCE(NULLIF(json_extract(peer.record_json,'$.source_turn_id'),''), 'legacy-turn:' || peer.id)=
                               COALESCE(NULLIF(json_extract(tasks.record_json,'$.source_turn_id'),''), 'legacy-turn:' || tasks.id)
                           AND json_type(peer.record_json,'$.kind') IS NULL
                    ) THEN 'primary' ELSE 'additional' END,
                    'evidence_refs',json('[]'),
                    'bound_at_ms',created_at_ms
                )
           FROM tasks
          WHERE json_type(record_json,'$.kind') IS NULL;

         UPDATE tasks
            SET record_json=json_remove(
                json_set(
                    record_json,
                    '$.kind','root',
                    '$.origin','system',
                    '$.origin_session_id',COALESCE(NULLIF(json_extract(record_json,'$.source_session_id'),''), 'legacy-session:' || id),
                    '$.origin_turn_id',COALESCE(NULLIF(json_extract(record_json,'$.source_turn_id'),''), 'legacy-turn:' || id),
                    '$.root_task_id',id,
                    '$.parent_task_id',NULL,
                    '$.predecessor_task_id',NULL,
                    '$.mission_assignment','explicit_locked',
                    '$.mission_assignment_revision',1,
                    '$.mission_assigned_by','migration/runtime-task-v6',
                    '$.mission_assignment_evidence_refs',json('[]')
                ),
                '$.source_session_id',
                '$.source_turn_id'
            )
          WHERE json_type(record_json,'$.kind') IS NULL;",
    )
    .map_err(|error| error.to_string())
}

fn sync_task_graph_refs_sqlite(
    conn: &rusqlite::Connection,
    task: &TaskAggregate,
) -> Result<(), String> {
    conn.execute(
        "DELETE FROM task_graph_refs WHERE task_id=?1",
        params![task.task_id],
    )
    .map_err(|error| error.to_string())?;
    for reference in &task.graph_refs {
        conn.execute(
            "INSERT INTO task_graph_refs(task_id, graph_id, graph_revision)
             VALUES (?1, ?2, ?3)",
            params![
                task.task_id,
                reference.graph_id,
                to_i64(reference.revision, "graph_revision")?,
            ],
        )
        .map_err(|error| error.to_string())?;
    }
    Ok(())
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

fn load_bindings_sqlite<P: rusqlite::Params>(
    conn: &rusqlite::Connection,
    sql: &str,
    params: P,
) -> Result<Vec<TaskTurnBinding>, String> {
    let mut statement = conn.prepare(sql).map_err(|error| error.to_string())?;
    let rows = statement
        .query_map(params, |row| row.get::<_, String>(0))
        .map_err(|error| error.to_string())?;
    rows.map(|row| {
        row.map_err(|error| error.to_string())
            .and_then(|raw| serde_json::from_str(&raw).map_err(|error| error.to_string()))
    })
    .collect()
}

fn task_turn_role_name(role: TaskTurnRole) -> &'static str {
    match role {
        TaskTurnRole::Primary => "primary",
        TaskTurnRole::Additional => "additional",
        TaskTurnRole::Review => "review",
        TaskTurnRole::Handoff => "handoff",
    }
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

fn prepare_task_creation(
    command: TaskCreateCommand,
) -> Result<(TaskAggregate, TaskMutation), String> {
    crate::task::validate_create_command(&command)?;
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
        kind: command.kind,
        origin: command.origin,
        origin_session_id: command.origin_session_id,
        origin_turn_id: command.origin_turn_id,
        root_task_id: command.root_task_id,
        parent_task_id: command.parent_task_id,
        predecessor_task_id: command.predecessor_task_id,
        mission_assignment: command.mission_assignment,
        mission_assignment_revision: 1,
        mission_assigned_by: command.mission_assigned_by,
        mission_assignment_evidence_refs: command.evidence_refs.clone(),
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
    Ok((aggregate, mutation))
}

pub fn same_immutable_task_creation(left: &TaskAggregate, right: &TaskAggregate) -> bool {
    left.task_id == right.task_id
        && left.mission_id == right.mission_id
        && left.kind == right.kind
        && left.origin == right.origin
        && left.origin_session_id == right.origin_session_id
        && left.origin_turn_id == right.origin_turn_id
        && left.root_task_id == right.root_task_id
        && left.parent_task_id == right.parent_task_id
        && left.predecessor_task_id == right.predecessor_task_id
        && left.objective == right.objective
        && left.execution_policy == right.execution_policy
}

#[must_use]
pub fn synthetic_evidence(ref_type: &str, id: impl Into<String>) -> EvidenceRef {
    EvidenceRef::observed(ref_type, id).with_source("runtime.task")
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::mission::MissionOrganizationAction;

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
                kind: TaskKind::Root,
                origin: TaskOrigin::User,
                origin_session_id: "session-a".to_string(),
                origin_turn_id: "turn-a".to_string(),
                root_task_id: id.to_string(),
                parent_task_id: None,
                predecessor_task_id: None,
                mission_assignment: TaskMissionAssignment::ExplicitLocked,
                mission_assigned_by: "test".to_string(),
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

    fn organization_decision(root: &str) -> MissionOrganizationDecision {
        MissionOrganizationDecision {
            decision_id: format!("mission-organization:{root}"),
            workspace_id: "workspace-1".to_string(),
            root_task_id: root.to_string(),
            affected_task_ids: vec![root.to_string()],
            action: MissionOrganizationAction::KeepDefault,
            target_mission_id: "mission-default".to_string(),
            proposed_objective: None,
            status: MissionOrganizationStatus::Pending,
            reason: "test organization".to_string(),
            candidate_count: 0,
            provider_invoked: false,
            provider_model: None,
            provider_input_tokens: 0,
            provider_output_tokens: 0,
            elapsed_ms: 0,
            rejected_reason: None,
            evidence_refs: vec![synthetic_evidence("task", root)],
            attempt: 0,
            next_attempt_at_ms: 1,
            claim_token: None,
            revision: 1,
            created_at_ms: 1,
            updated_at_ms: 1,
        }
    }

    #[test]
    fn organization_replay_is_anchored_to_immutable_root_not_mutable_cluster_members() {
        let handle = temp_handle("organization-root-replay");
        let service = TaskAggregateService::open_storage_handle(&handle).expect("task service");
        let original = organization_decision("task-root");
        service
            .save_organization_decision(&original, None)
            .expect("save original decision");

        let mut clustered_replay = original.clone();
        clustered_replay.affected_task_ids =
            vec!["task-root".to_string(), "task-related".to_string()];
        let retained = service
            .save_organization_decision(&clustered_replay, None)
            .expect("same Root replay is idempotent");
        assert_eq!(retained, original);

        let mut foreign_root = clustered_replay;
        foreign_root.root_task_id = "task-foreign".to_string();
        assert!(service
            .save_organization_decision(&foreign_root, None)
            .is_err());
    }

    #[test]
    fn sqlite_startup_upgrades_legacy_task_into_locked_root_with_turn_binding() {
        let path = std::env::temp_dir().join(format!(
            "cowd-runtime-task-legacy-{}.db",
            uuid::Uuid::new_v4()
        ));
        let connection = rusqlite::Connection::open(&path).expect("legacy database");
        connection
            .execute_batch(
                "CREATE TABLE tasks (
                    id TEXT PRIMARY KEY NOT NULL,
                    status TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    record_json TEXT NOT NULL
                );",
            )
            .expect("legacy schema");
        let legacy = serde_json::json!({
            "task_id": "legacy-task",
            "mission_id": "legacy-mission",
            "source_session_id": "legacy-session",
            "source_turn_id": "legacy-turn",
            "objective": "preserve the old task",
            "status": "running",
            "revision": 1,
            "current_phase_id": null,
            "phases": [],
            "execution_policy": {
                "yolo_mode": false,
                "max_failures_before_block": 3
            },
            "failure_count": 0,
            "blocker_reason": null,
            "strategy_ref": null,
            "graph_refs": [],
            "created_at_ms": 10,
            "updated_at_ms": 11
        });
        connection
            .execute(
                "INSERT INTO tasks(id,status,created_at_ms,updated_at_ms,record_json)
                 VALUES(?1,?2,?3,?4,?5)",
                params!["legacy-task", "running", 10_i64, 11_i64, legacy.to_string()],
            )
            .expect("legacy row");
        drop(connection);

        let service = TaskAggregateService::open(path.clone()).expect("upgraded task service");
        let task = service
            .get("legacy-task")
            .expect("task query")
            .expect("legacy task retained");
        assert_eq!(task.kind, TaskKind::Root);
        assert_eq!(task.origin, TaskOrigin::System);
        assert_eq!(task.root_task_id, "legacy-task");
        assert_eq!(
            task.mission_assignment,
            TaskMissionAssignment::ExplicitLocked
        );
        assert_eq!(task.mission_id, "legacy-mission");
        let bindings = service
            .bindings_for_task("legacy-task")
            .expect("legacy binding query");
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].session_id, "legacy-session");
        assert_eq!(bindings[0].turn_id, "legacy-turn");
        assert_eq!(bindings[0].role, TaskTurnRole::Primary);

        drop(service);
        let reopened = TaskAggregateService::open(path.clone()).expect("idempotent reopen");
        assert_eq!(reopened.list().expect("reopened tasks").len(), 1);
        assert_eq!(
            reopened
                .bindings_for_task("legacy-task")
                .expect("reopened bindings")
                .len(),
            1
        );
        let _ = std::fs::remove_file(path);
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
        let value = serde_json::to_value(&linked.aggregate).expect("serialize task");
        assert!(value.get("execution_graph").is_none());

        let replay = service
            .link_graph(
                "task-graph",
                linked.aggregate.revision,
                "graph-1",
                7,
                vec![synthetic_evidence("execution_graph", "graph-1")],
            )
            .expect("idempotent graph replay");
        assert_eq!(replay.aggregate.revision, linked.aggregate.revision);
        assert_eq!(replay.outbox, linked.outbox);
        assert_eq!(service.backend.list_outbox().expect("outbox").len(), 2);
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
        assert_eq!(loaded.origin_session_id, "session-a");
        assert_eq!(loaded.mission_id, "mission-1");
    }

    #[test]
    fn assignment_operation_id_cannot_be_reused_for_a_different_command() {
        let handle = temp_handle("assignment-replay");
        let service = TaskAggregateService::open_storage_handle(&handle).expect("task service");
        let created = create(&service, "task-assignment-replay");
        let command = TaskMissionAssignmentCommand {
            operation_id: "assignment-operation".to_string(),
            workspace_id: "workspace".to_string(),
            task_ids: vec![created.aggregate.task_id.clone()],
            target_mission_id: "mission-2".to_string(),
            assignment: TaskMissionAssignment::ExplicitLocked,
            actor: "test".to_string(),
            expected_task_revisions: [(created.aggregate.task_id.clone(), 1)]
                .into_iter()
                .collect(),
            evidence_refs: vec![synthetic_evidence("test", "assignment")],
        };
        let receipt = service
            .assign_mission_batch(&command)
            .expect("first assignment");
        assert_eq!(
            service
                .assign_mission_batch(&command)
                .expect("identical replay"),
            receipt
        );

        let mut changed = command;
        changed.target_mission_id = "mission-3".to_string();
        assert!(service
            .assign_mission_batch(&changed)
            .is_err_and(|error| error.contains("reused with a different command")));
    }
}
