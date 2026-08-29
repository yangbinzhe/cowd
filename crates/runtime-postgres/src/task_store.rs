//! PostgreSQL task aggregate adapter and migration pipeline.

use super::*;

/// Complete PostgreSQL implementation of the Task control-plane backend.
///
/// The store locks only the task being updated. Independent task lifecycles
/// can therefore use separate PostgreSQL connections concurrently; task-level
/// transitions remain atomic even across gateway processes.
#[derive(Clone, Debug)]
pub struct PostgresTaskStore {
    executor: PostgresExecutor,
}

/// Immutable proof written only after an explicit quiesced task copy reaches
/// canonical digest equality. It intentionally carries no backend URL/path.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TaskMigrationManifest {
    pub domain: String,
    pub source_digest: String,
    pub target_digest: String,
    pub task_count: usize,
}

impl PostgresTaskStore {
    pub fn new(executor: PostgresExecutor) -> Result<Self, String> {
        executor
            .apply_migrations(TASK_DOMAIN, TASK_MIGRATIONS)
            .map_err(|error| error.to_string())?;
        Ok(Self { executor })
    }

    pub fn connect(
        config: PostgresConnectionConfig,
        resolver: &dyn SecretRefResolver,
    ) -> Result<Self, String> {
        Self::new(PostgresExecutor::connect(config, resolver).map_err(|error| error.to_string())?)
    }

    #[must_use]
    pub fn executor(&self) -> &PostgresExecutor {
        &self.executor
    }

    #[must_use]
    pub fn into_task_service(self) -> TaskAggregateService {
        TaskAggregateService::from_backend(Arc::new(self))
    }
}

impl TaskStoreBackend for PostgresTaskStore {
    fn list(&self) -> Result<Vec<TaskAggregate>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let rows = connection
            .query(
                "SELECT record_json FROM runtime_tasks ORDER BY created_at_ms ASC, task_id ASC",
                &[],
            )
            .map_err(|error| error.to_string())?;
        rows.iter().map(task_record_from_row).collect()
    }

    fn get(&self, task_id: &str) -> Result<Option<TaskAggregate>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        connection
            .query_opt(
                "SELECT record_json FROM runtime_tasks WHERE task_id=$1",
                &[&task_id],
            )
            .map_err(|error| error.to_string())?
            .map(|row| task_record_from_row(&row))
            .transpose()
    }

    fn organization_candidates(&self, limit: usize) -> Result<Vec<TaskAggregate>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = connection
            .query(
                "SELECT record_json FROM runtime_tasks
                  WHERE status IN ('pending','running','reviewing','blocked')
                    AND record_json ->> 'kind' = 'root'
                    AND record_json ->> 'origin' <> 'system'
                    AND record_json ->> 'mission_assignment' <> 'explicit_locked'
                  ORDER BY updated_at_ms DESC,task_id ASC LIMIT $1",
                &[&limit],
            )
            .map_err(|error| error.to_string())?;
        rows.iter().map(task_record_from_row).collect()
    }

    fn unorganized_candidates(&self, limit: usize) -> Result<Vec<TaskAggregate>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = connection
            .query(
                "SELECT task.record_json FROM runtime_tasks AS task
                  WHERE task.status IN ('pending','running','reviewing','blocked')
                    AND task.record_json ->> 'kind' = 'root'
                    AND task.record_json ->> 'origin' <> 'system'
                    AND task.record_json ->> 'mission_assignment' <> 'explicit_locked'
                    AND NOT EXISTS (
                        SELECT 1 FROM runtime_mission_organization_decisions AS decision
                         WHERE decision.decision_id = 'mission-organization:' || task.task_id
                    )
                  ORDER BY task.updated_at_ms DESC,task.task_id ASC LIMIT $1",
                &[&limit],
            )
            .map_err(|error| error.to_string())?;
        rows.iter().map(task_record_from_row).collect()
    }

    fn open_root_candidates(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<TaskAggregate>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = connection
            .query(
                "SELECT DISTINCT task.record_json,task.updated_at_ms,task.task_id
                   FROM runtime_task_turn_bindings AS binding
                   JOIN runtime_tasks AS task ON task.task_id=binding.task_id
                  WHERE binding.session_id=$1
                    AND task.status IN ('pending','running','reviewing','blocked')
                    AND task.record_json ->> 'kind' = 'root'
                  ORDER BY task.updated_at_ms DESC,task.task_id ASC LIMIT $2",
                &[&session_id, &limit],
            )
            .map_err(|error| error.to_string())?;
        rows.iter().map(task_record_from_row).collect()
    }

    fn for_graphs(&self, graph_ids: &[String]) -> Result<Vec<TaskAggregate>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let mut tasks = std::collections::BTreeMap::new();
        for graph_id in graph_ids {
            let rows = connection
                .query(
                    "SELECT task.record_json
                       FROM runtime_task_graph_refs AS reference
                       JOIN runtime_tasks AS task ON task.task_id=reference.task_id
                      WHERE reference.graph_id=$1",
                    &[graph_id],
                )
                .map_err(|error| error.to_string())?;
            for row in rows {
                let task = task_record_from_row(&row)?;
                tasks.insert(task.task_id.clone(), task);
            }
        }
        Ok(tasks.into_values().collect())
    }

    fn bind_turn(&self, binding: &TaskTurnBinding) -> Result<TaskTurnBinding, String> {
        runtime::task::validate_binding(binding)?;
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|error| error.to_string())?;
        let record_json = serde_json::to_value(binding).map_err(|error| error.to_string())?;
        connection
            .execute(
                "INSERT INTO runtime_task_turn_bindings(
                    binding_id,task_id,session_id,turn_id,role,input_id,bound_at_ms,record_json
                 ) VALUES($1,$2,$3,$4,$5,$6,$7,$8)
                 ON CONFLICT(task_id,session_id,turn_id) DO NOTHING",
                &[
                    &binding.binding_id,
                    &binding.task_id,
                    &binding.session_id,
                    &binding.turn_id,
                    &task_turn_role_name(binding.role),
                    &binding.input_id,
                    &task_time_i64(binding.bound_at_ms, "bound_at_ms")?,
                    &record_json,
                ],
            )
            .map_err(|error| error.to_string())?;
        let row = connection
            .query_one(
                "SELECT record_json FROM runtime_task_turn_bindings
                  WHERE task_id=$1 AND session_id=$2 AND turn_id=$3",
                &[&binding.task_id, &binding.session_id, &binding.turn_id],
            )
            .map_err(|error| error.to_string())?;
        let stored = task_binding_from_row(&row)?;
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
        runtime::task::validate_binding(binding)?;
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|error| error.to_string())?;
        let mut transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let lock_key = format!("cowd-runtime-task:{}", aggregate.task_id);
        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                &[&lock_key],
            )
            .map_err(|error| error.to_string())?;
        let current = transaction
            .query_opt(
                "SELECT record_json FROM runtime_tasks WHERE task_id=$1 FOR UPDATE",
                &[&aggregate.task_id],
            )
            .map_err(|error| error.to_string())?
            .map(|row| task_record_from_row(&row))
            .transpose()?;
        let (stored_task, outbox) = if let Some(current) = current {
            if !runtime::task::same_immutable_task_creation(&current, aggregate) {
                return Err(format!(
                    "task id `{}` is already bound to different immutable creation data",
                    aggregate.task_id
                ));
            }
            let row = transaction
                .query_one(
                    "SELECT record_json FROM runtime_task_evidence_outbox
                      WHERE task_id=$1 AND revision=$2",
                    &[
                        &current.task_id,
                        &task_time_i64(current.revision, "revision")?,
                    ],
                )
                .map_err(|error| error.to_string())?;
            (current, task_outbox_from_row(&row)?)
        } else {
            let outbox = validate_backend_mutation(&aggregate.task_id, None, aggregate, mutation)?
                .ok_or_else(|| "Task creation requires an evidence outbox".to_string())?;
            let record_json = serde_json::to_value(aggregate).map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "INSERT INTO runtime_tasks(
                        task_id,status,created_at_ms,updated_at_ms,record_json
                     ) VALUES($1,$2,$3,$4,$5)",
                    &[
                        &aggregate.task_id,
                        &aggregate.status.as_str(),
                        &task_time_i64(aggregate.created_at_ms, "created_at_ms")?,
                        &task_time_i64(aggregate.updated_at_ms, "updated_at_ms")?,
                        &record_json,
                    ],
                )
                .map_err(|error| error.to_string())?;
            sync_task_graph_refs_postgres(&mut transaction, aggregate)?;
            let outbox_json = serde_json::to_value(&outbox).map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "INSERT INTO runtime_task_evidence_outbox(
                        outbox_id,task_id,revision,event_kind,created_at_ms,record_json
                     ) VALUES($1,$2,$3,$4,$5,$6)",
                    &[
                        &outbox.outbox_id,
                        &outbox.task_id,
                        &task_time_i64(outbox.revision, "revision")?,
                        &outbox.event_kind,
                        &task_time_i64(outbox.created_at_ms, "created_at_ms")?,
                        &outbox_json,
                    ],
                )
                .map_err(|error| error.to_string())?;
            (aggregate.clone(), outbox)
        };
        let binding_json = serde_json::to_value(binding).map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO runtime_task_turn_bindings(
                    binding_id,task_id,session_id,turn_id,role,input_id,bound_at_ms,record_json
                 ) VALUES($1,$2,$3,$4,$5,$6,$7,$8)
                 ON CONFLICT(task_id,session_id,turn_id) DO NOTHING",
                &[
                    &binding.binding_id,
                    &binding.task_id,
                    &binding.session_id,
                    &binding.turn_id,
                    &task_turn_role_name(binding.role),
                    &binding.input_id,
                    &task_time_i64(binding.bound_at_ms, "bound_at_ms")?,
                    &binding_json,
                ],
            )
            .map_err(|error| error.to_string())?;
        let row = transaction
            .query_one(
                "SELECT record_json FROM runtime_task_turn_bindings
                  WHERE task_id=$1 AND session_id=$2 AND turn_id=$3",
                &[&binding.task_id, &binding.session_id, &binding.turn_id],
            )
            .map_err(|error| error.to_string())?;
        let stored_binding = task_binding_from_row(&row)?;
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
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let rows = connection
            .query(
                "SELECT record_json FROM runtime_task_turn_bindings
                  WHERE task_id=$1 ORDER BY bound_at_ms ASC,binding_id ASC",
                &[&task_id],
            )
            .map_err(|error| error.to_string())?;
        rows.iter().map(task_binding_from_row).collect()
    }

    fn bindings_for_turn(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> Result<Vec<TaskTurnBinding>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let rows = connection
            .query(
                "SELECT record_json FROM runtime_task_turn_bindings
                  WHERE session_id=$1 AND turn_id=$2
                  ORDER BY CASE role WHEN 'primary' THEN 0 ELSE 1 END,
                           bound_at_ms ASC,binding_id ASC",
                &[&session_id, &turn_id],
            )
            .map_err(|error| error.to_string())?;
        rows.iter().map(task_binding_from_row).collect()
    }

    fn assign_mission_batch(
        &self,
        command: &TaskMissionAssignmentCommand,
    ) -> Result<TaskMissionAssignmentReceipt, String> {
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|error| error.to_string())?;
        let mut transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                &[&format!(
                    "cowd-task-mission-assignment:{}",
                    command.operation_id
                )],
            )
            .map_err(|error| error.to_string())?;
        if let Some(row) = transaction
            .query_opt(
                "SELECT record_json FROM runtime_task_mission_assignment_outbox WHERE operation_id=$1",
                &[&command.operation_id],
            )
            .map_err(|error| error.to_string())?
        {
            let value: Value = row.try_get(0).map_err(|error| error.to_string())?;
            let record: TaskMissionAssignmentOutboxRecord =
                serde_json::from_value(value).map_err(|error| error.to_string())?;
            validate_task_assignment_replay(command, &record.receipt)?;
            return Ok(record.receipt);
        }
        if command.task_ids.is_empty()
            || command.expected_task_revisions.len() != command.task_ids.len()
        {
            return Err(
                "task mission assignment requires Tasks and expected revisions".to_string(),
            );
        }
        let applied_at_ms = task_now_ms();
        let mut updated = Vec::with_capacity(command.task_ids.len());
        for task_id in &command.task_ids {
            let task_id_value = task_id.as_str();
            let row = transaction
                .query_opt(
                    "SELECT record_json FROM runtime_tasks WHERE task_id=$1 FOR UPDATE",
                    &[&task_id_value],
                )
                .map_err(|error| error.to_string())?
                .ok_or_else(|| format!("task `{task_id_value}` not found"))?;
            let mut task = task_record_from_row(&row)?;
            let expected = command
                .expected_task_revisions
                .get(task_id_value)
                .copied()
                .ok_or_else(|| format!("task `{task_id_value}` has no expected revision"))?;
            if task.revision != expected {
                return Err(format!(
                    "task `{task_id_value}` revision conflict: expected {expected}, actual {}",
                    task.revision
                ));
            }
            if task.mission_assignment == TaskMissionAssignment::ExplicitLocked
                && command.assignment != TaskMissionAssignment::ExplicitLocked
            {
                return Err(format!(
                    "task `{task_id_value}` has an explicit Mission lock"
                ));
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
            .collect::<BTreeSet<_>>();
        for task in &updated {
            if task.kind == TaskKind::Delegated && !selected.contains(task.root_task_id.as_str()) {
                let row = transaction
                    .query_one(
                        "SELECT record_json FROM runtime_tasks WHERE task_id=$1 FOR UPDATE",
                        &[&task.root_task_id],
                    )
                    .map_err(|error| error.to_string())?;
                let root = task_record_from_row(&row)?;
                if root.mission_id != command.target_mission_id {
                    return Err(format!(
                        "delegated task `{}` cannot leave root task `{}` in another Mission",
                        task.task_id, task.root_task_id
                    ));
                }
            }
        }
        let mut task_revisions = BTreeMap::new();
        for task in &updated {
            let record_json = serde_json::to_value(task).map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "UPDATE runtime_tasks SET status=$2,updated_at_ms=$3,record_json=$4 WHERE task_id=$1",
                    &[
                        &task.task_id,
                        &task.status.as_str(),
                        &task_time_i64(task.updated_at_ms, "updated_at_ms")?,
                        &record_json,
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
            let outbox_json = serde_json::to_value(&outbox).map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "INSERT INTO runtime_task_evidence_outbox(
                        outbox_id,task_id,revision,event_kind,created_at_ms,record_json
                     ) VALUES($1,$2,$3,$4,$5,$6)",
                    &[
                        &outbox.outbox_id,
                        &outbox.task_id,
                        &task_time_i64(outbox.revision, "revision")?,
                        &outbox.event_kind,
                        &task_time_i64(outbox.created_at_ms, "created_at_ms")?,
                        &outbox_json,
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
        let record_json = serde_json::to_value(&record).map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO runtime_task_mission_assignment_outbox(
                    operation_id,created_at_ms,record_json
                 ) VALUES($1,$2,$3)",
                &[
                    &record.operation_id,
                    &task_time_i64(record.created_at_ms, "created_at_ms")?,
                    &record_json,
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
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        connection
            .query_opt(
                "SELECT record_json FROM runtime_task_mission_assignment_outbox WHERE operation_id=$1",
                &[&operation_id],
            )
            .map_err(|error| error.to_string())?
            .map(|row| {
                let value: Value = row.try_get(0).map_err(|error| error.to_string())?;
                serde_json::from_value::<TaskMissionAssignmentOutboxRecord>(value)
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
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|error| error.to_string())?;
        let mut transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let existing = transaction
            .query_opt(
                "SELECT record_json FROM runtime_mission_organization_decisions
                  WHERE decision_id=$1 FOR UPDATE",
                &[&decision.decision_id],
            )
            .map_err(|error| error.to_string())?
            .map(|row| {
                let value: Value = row.try_get(0).map_err(|error| error.to_string())?;
                serde_json::from_value::<MissionOrganizationDecision>(value)
                    .map_err(|error| error.to_string())
            })
            .transpose()?;
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
        let record_json = serde_json::to_value(decision).map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO runtime_mission_organization_decisions(
                    decision_id,status,next_attempt_at_ms,created_at_ms,updated_at_ms,record_json
                 ) VALUES($1,$2,$3,$4,$5,$6)
                 ON CONFLICT(decision_id) DO UPDATE SET
                    status=EXCLUDED.status,next_attempt_at_ms=EXCLUDED.next_attempt_at_ms,
                    updated_at_ms=EXCLUDED.updated_at_ms,record_json=EXCLUDED.record_json",
                &[
                    &decision.decision_id,
                    &task_organization_status_name(decision.status),
                    &task_time_i64(decision.next_attempt_at_ms, "next_attempt_at_ms")?,
                    &task_time_i64(decision.created_at_ms, "created_at_ms")?,
                    &task_time_i64(decision.updated_at_ms, "updated_at_ms")?,
                    &record_json,
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
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = if let Some(status) = status {
            connection
                .query(
                    "SELECT record_json FROM runtime_mission_organization_decisions
                      WHERE status=$1 ORDER BY created_at_ms ASC,decision_id ASC LIMIT $2",
                    &[&task_organization_status_name(status), &limit],
                )
                .map_err(|error| error.to_string())?
        } else {
            connection
                .query(
                    "SELECT record_json FROM runtime_mission_organization_decisions
                      ORDER BY created_at_ms ASC,decision_id ASC LIMIT $1",
                    &[&limit],
                )
                .map_err(|error| error.to_string())?
        };
        rows.into_iter()
            .map(|row| {
                let value: Value = row.try_get(0).map_err(|error| error.to_string())?;
                serde_json::from_value(value).map_err(|error| error.to_string())
            })
            .collect()
    }

    fn mutate_task(
        &self,
        task_id: &str,
        mutation: &TaskMutation,
        updater: &mut dyn FnMut(Option<TaskAggregate>) -> Result<TaskAggregate, String>,
    ) -> Result<TaskMutationResult, String> {
        if task_id.trim().is_empty() {
            return Err("task id is required".to_string());
        }
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|error| error.to_string())?;
        let mut transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let lock_key = format!("cowd-runtime-task:{task_id}");
        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                &[&lock_key],
            )
            .map_err(|error| error.to_string())?;
        let current = transaction
            .query_opt(
                "SELECT record_json FROM runtime_tasks WHERE task_id=$1 FOR UPDATE",
                &[&task_id],
            )
            .map_err(|error| error.to_string())?
            .map(|row| task_record_from_row(&row))
            .transpose()?;
        let next = updater(current.clone())?;
        if current.as_ref() == Some(&next) {
            validate_task_aggregate_for_backend(&next)?;
            let revision = task_time_i64(next.revision, "revision")?;
            let row = transaction
                .query_opt(
                    "SELECT record_json FROM runtime_task_evidence_outbox
                     WHERE task_id=$1 AND revision=$2",
                    &[&task_id, &revision],
                )
                .map_err(|error| error.to_string())?
                .ok_or_else(|| {
                    format!(
                        "idempotent task replay `{task_id}` revision {} has no durable outbox",
                        next.revision
                    )
                })?;
            let outbox = task_outbox_from_row(&row)?;
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
        let record_json = serde_json::to_value(&next).map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO runtime_tasks
                    (task_id, status, created_at_ms, updated_at_ms, record_json)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT(task_id) DO UPDATE SET
                    status=EXCLUDED.status,
                    created_at_ms=EXCLUDED.created_at_ms,
                    updated_at_ms=EXCLUDED.updated_at_ms,
                    record_json=EXCLUDED.record_json",
                &[
                    &next.task_id,
                    &next.status.as_str(),
                    &task_time_i64(next.created_at_ms, "created_at_ms")?,
                    &task_time_i64(next.updated_at_ms, "updated_at_ms")?,
                    &record_json,
                ],
            )
            .map_err(|error| error.to_string())?;
        sync_task_graph_refs_postgres(&mut transaction, &next)?;
        let outbox = outbox.ok_or_else(|| {
            format!("task `{task_id}` changed without a durable evidence outbox record")
        })?;
        let outbox_json = serde_json::to_value(&outbox).map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO runtime_task_evidence_outbox
                    (outbox_id, task_id, revision, event_kind, created_at_ms, record_json)
                 VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &outbox.outbox_id,
                    &outbox.task_id,
                    &task_time_i64(outbox.revision, "revision")?,
                    &outbox.event_kind,
                    &task_time_i64(outbox.created_at_ms, "created_at_ms")?,
                    &outbox_json,
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
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let limit = i64::try_from(limit.min(i64::MAX as usize)).unwrap_or(i64::MAX);
        let rows = if let Some(task_id) = task_id {
            connection
                .query(
                    "SELECT record_json FROM runtime_task_evidence_outbox
                     WHERE projected_at_ms IS NULL AND task_id=$1
                     ORDER BY revision ASC LIMIT $2",
                    &[&task_id, &limit],
                )
                .map_err(|error| error.to_string())?
        } else {
            connection
                .query(
                    "SELECT record_json FROM runtime_task_evidence_outbox
                     WHERE projected_at_ms IS NULL
                     ORDER BY created_at_ms ASC, outbox_id ASC LIMIT $1",
                    &[&limit],
                )
                .map_err(|error| error.to_string())?
        };
        rows.iter().map(task_outbox_from_row).collect()
    }

    fn list_outbox(&self) -> Result<Vec<TaskEvidenceOutboxRecord>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let rows = connection
            .query(
                "SELECT record_json FROM runtime_task_evidence_outbox
                 ORDER BY created_at_ms ASC, outbox_id ASC",
                &[],
            )
            .map_err(|error| error.to_string())?;
        rows.iter().map(task_outbox_from_row).collect()
    }

    fn list_assignment_outbox(&self) -> Result<Vec<TaskMissionAssignmentOutboxRecord>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let rows = connection
            .query(
                "SELECT record_json FROM runtime_task_mission_assignment_outbox
                 ORDER BY created_at_ms ASC, operation_id ASC",
                &[],
            )
            .map_err(|error| error.to_string())?;
        rows.into_iter()
            .map(|row| {
                let value: Value = row.try_get(0).map_err(|error| error.to_string())?;
                serde_json::from_value(value).map_err(|error| error.to_string())
            })
            .collect()
    }

    fn mark_outbox_projected(&self, outbox_id: &str, projected_at_ms: u64) -> Result<(), String> {
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|error| error.to_string())?;
        let mut transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let row = transaction
            .query_opt(
                "SELECT record_json FROM runtime_task_evidence_outbox
                 WHERE outbox_id=$1 FOR UPDATE",
                &[&outbox_id],
            )
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("task evidence outbox `{outbox_id}` not found"))?;
        let mut record = task_outbox_from_row(&row)?;
        record.projected_at_ms = Some(projected_at_ms);
        let record_json = serde_json::to_value(&record).map_err(|error| error.to_string())?;
        transaction
            .execute(
                "UPDATE runtime_task_evidence_outbox
                 SET projected_at_ms=$2, record_json=$3 WHERE outbox_id=$1",
                &[
                    &outbox_id,
                    &task_time_i64(projected_at_ms, "projected_at_ms")?,
                    &record_json,
                ],
            )
            .map_err(|error| error.to_string())?;
        transaction.commit().map_err(|error| error.to_string())
    }

    fn import_migration_snapshot(&self, snapshot: &TaskStoreSnapshot) -> Result<(), String> {
        snapshot.validate()?;
        let mut connection = self
            .executor
            .checkout_background()
            .map_err(|error| error.to_string())?;
        let mut transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        transaction
            .batch_execute(
                "LOCK TABLE runtime_tasks IN EXCLUSIVE MODE;
                 LOCK TABLE runtime_task_graph_refs IN EXCLUSIVE MODE;
                 LOCK TABLE runtime_task_evidence_outbox IN EXCLUSIVE MODE;
                 LOCK TABLE runtime_task_turn_bindings IN EXCLUSIVE MODE;
                 LOCK TABLE runtime_task_mission_assignment_outbox IN EXCLUSIVE MODE;
                 LOCK TABLE runtime_mission_organization_decisions IN EXCLUSIVE MODE",
            )
            .map_err(|error| error.to_string())?;
        let existing_tasks: i64 = transaction
            .query_one("SELECT COUNT(*) FROM runtime_tasks", &[])
            .map_err(|error| error.to_string())?
            .get(0);
        let existing_outbox: i64 = transaction
            .query_one("SELECT COUNT(*) FROM runtime_task_evidence_outbox", &[])
            .map_err(|error| error.to_string())?
            .get(0);
        let existing_bindings: i64 = transaction
            .query_one("SELECT COUNT(*) FROM runtime_task_turn_bindings", &[])
            .map_err(|error| error.to_string())?
            .get(0);
        let existing_assignments: i64 = transaction
            .query_one(
                "SELECT COUNT(*) FROM runtime_task_mission_assignment_outbox",
                &[],
            )
            .map_err(|error| error.to_string())?
            .get(0);
        let existing_decisions: i64 = transaction
            .query_one(
                "SELECT COUNT(*) FROM runtime_mission_organization_decisions",
                &[],
            )
            .map_err(|error| error.to_string())?
            .get(0);
        if existing_tasks != 0
            || existing_bindings != 0
            || existing_outbox != 0
            || existing_assignments != 0
            || existing_decisions != 0
        {
            return Err("task migration target must be empty".to_string());
        }
        for task in &snapshot.tasks {
            let record_json = serde_json::to_value(task).map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "INSERT INTO runtime_tasks
                        (task_id, status, created_at_ms, updated_at_ms, record_json)
                     VALUES ($1, $2, $3, $4, $5)",
                    &[
                        &task.task_id,
                        &task.status.as_str(),
                        &task_time_i64(task.created_at_ms, "created_at_ms")?,
                        &task_time_i64(task.updated_at_ms, "updated_at_ms")?,
                        &record_json,
                    ],
                )
                .map_err(|error| error.to_string())?;
            sync_task_graph_refs_postgres(&mut transaction, task)?;
        }
        for binding in &snapshot.bindings {
            let record_json = serde_json::to_value(binding).map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "INSERT INTO runtime_task_turn_bindings(
                        binding_id,task_id,session_id,turn_id,role,input_id,bound_at_ms,record_json
                     ) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
                    &[
                        &binding.binding_id,
                        &binding.task_id,
                        &binding.session_id,
                        &binding.turn_id,
                        &task_turn_role_name(binding.role),
                        &binding.input_id,
                        &task_time_i64(binding.bound_at_ms, "bound_at_ms")?,
                        &record_json,
                    ],
                )
                .map_err(|error| error.to_string())?;
        }
        for record in &snapshot.outbox {
            let record_json = serde_json::to_value(record).map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "INSERT INTO runtime_task_evidence_outbox
                        (outbox_id, task_id, revision, event_kind, created_at_ms,
                         projected_at_ms, record_json)
                     VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    &[
                        &record.outbox_id,
                        &record.task_id,
                        &task_time_i64(record.revision, "revision")?,
                        &record.event_kind,
                        &task_time_i64(record.created_at_ms, "created_at_ms")?,
                        &record
                            .projected_at_ms
                            .map(|value| task_time_i64(value, "projected_at_ms"))
                            .transpose()?,
                        &record_json,
                    ],
                )
                .map_err(|error| error.to_string())?;
        }
        for record in &snapshot.assignment_outbox {
            let record_json = serde_json::to_value(record).map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "INSERT INTO runtime_task_mission_assignment_outbox(
                        operation_id,created_at_ms,projected_at_ms,record_json
                     ) VALUES($1,$2,$3,$4)",
                    &[
                        &record.operation_id,
                        &task_time_i64(record.created_at_ms, "created_at_ms")?,
                        &record
                            .projected_at_ms
                            .map(|value| task_time_i64(value, "projected_at_ms"))
                            .transpose()?,
                        &record_json,
                    ],
                )
                .map_err(|error| error.to_string())?;
        }
        for decision in &snapshot.organization_decisions {
            let record_json = serde_json::to_value(decision).map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "INSERT INTO runtime_mission_organization_decisions(
                        decision_id,status,next_attempt_at_ms,created_at_ms,updated_at_ms,record_json
                     ) VALUES($1,$2,$3,$4,$5,$6)",
                    &[
                        &decision.decision_id,
                        &task_organization_status_name(decision.status),
                        &task_time_i64(decision.next_attempt_at_ms, "next_attempt_at_ms")?,
                        &task_time_i64(decision.created_at_ms, "created_at_ms")?,
                        &task_time_i64(decision.updated_at_ms, "updated_at_ms")?,
                        &record_json,
                    ],
                )
                .map_err(|error| error.to_string())?;
        }
        transaction.commit().map_err(|error| error.to_string())
    }
}

/// Copy a quiesced Task control plane exactly once, prove canonical digest
/// equality, then atomically write a backend-neutral cutover manifest.
pub fn copy_quiesced_task_service(
    source: &TaskAggregateService,
    target: &TaskAggregateService,
    manifest_path: impl AsRef<Path>,
) -> Result<TaskMigrationManifest, String> {
    let snapshot = source.export_migration_snapshot()?;
    snapshot.validate()?;
    let source_digest = snapshot.canonical_digest()?;
    target.import_migration_snapshot(&snapshot)?;
    let target_snapshot = target.export_migration_snapshot()?;
    let target_digest = target_snapshot.canonical_digest()?;
    if source_digest != target_digest {
        return Err("task migration digest mismatch".to_string());
    }
    let manifest = TaskMigrationManifest {
        domain: TASK_DOMAIN.to_string(),
        source_digest,
        target_digest,
        task_count: snapshot.tasks.len(),
    };
    write_task_migration_manifest(manifest_path.as_ref(), &manifest)?;
    Ok(manifest)
}

fn sync_task_graph_refs_postgres(
    transaction: &mut impl PostgresClient,
    task: &TaskAggregate,
) -> Result<(), String> {
    transaction
        .execute(
            "DELETE FROM runtime_task_graph_refs WHERE task_id=$1",
            &[&task.task_id],
        )
        .map_err(|error| error.to_string())?;
    for reference in &task.graph_refs {
        transaction
            .execute(
                "INSERT INTO runtime_task_graph_refs(task_id, graph_id, graph_revision)
                 VALUES ($1, $2, $3)",
                &[
                    &task.task_id,
                    &reference.graph_id,
                    &task_time_i64(reference.revision, "graph_revision")?,
                ],
            )
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn task_record_from_row(row: &Row) -> Result<TaskAggregate, String> {
    let record_json: Value = row.try_get(0).map_err(|error| error.to_string())?;
    serde_json::from_value(record_json).map_err(|error| error.to_string())
}

fn task_outbox_from_row(row: &Row) -> Result<TaskEvidenceOutboxRecord, String> {
    let record_json: Value = row.try_get(0).map_err(|error| error.to_string())?;
    serde_json::from_value(record_json).map_err(|error| error.to_string())
}

fn task_binding_from_row(row: &Row) -> Result<TaskTurnBinding, String> {
    let record_json: Value = row.try_get(0).map_err(|error| error.to_string())?;
    serde_json::from_value(record_json).map_err(|error| error.to_string())
}

const fn task_turn_role_name(role: runtime::TaskTurnRole) -> &'static str {
    match role {
        runtime::TaskTurnRole::Primary => "primary",
        runtime::TaskTurnRole::Additional => "additional",
        runtime::TaskTurnRole::Review => "review",
        runtime::TaskTurnRole::Handoff => "handoff",
    }
}

fn task_time_i64(value: u64, field: &str) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("task `{field}` exceeds i64"))
}

fn task_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn validate_task_assignment_replay(
    command: &TaskMissionAssignmentCommand,
    receipt: &TaskMissionAssignmentReceipt,
) -> Result<(), String> {
    let requested = command.task_ids.iter().collect::<BTreeSet<_>>();
    let committed = receipt.task_revisions.keys().collect::<BTreeSet<_>>();
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

const fn task_organization_status_name(status: MissionOrganizationStatus) -> &'static str {
    match status {
        MissionOrganizationStatus::Pending => "pending",
        MissionOrganizationStatus::Claimed => "claimed",
        MissionOrganizationStatus::Applied => "applied",
        MissionOrganizationStatus::Rejected => "rejected",
        MissionOrganizationStatus::Failed => "failed",
    }
}

fn write_task_migration_manifest(
    manifest_path: &Path,
    manifest: &TaskMigrationManifest,
) -> Result<(), String> {
    if let Some(parent) = manifest_path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let temporary_path = PathBuf::from(format!(
        "{}.{}.tmp",
        manifest_path.display(),
        uuid::Uuid::new_v4()
    ));
    fs::write(
        &temporary_path,
        serde_json::to_vec_pretty(manifest).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    fs::rename(temporary_path, manifest_path).map_err(|error| error.to_string())
}
