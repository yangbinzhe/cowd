//! Runtime-owned Task query and command port.

use std::sync::Arc;

use harness_contract::execution_graph::ExecutionGraphProjection;
use harness_contract::mission::{
    MissionOrganizationDecision, MissionOrganizationStatus, MissionStatus,
    TaskMissionAssignmentCommand, TaskMissionAssignmentPreview, TaskMissionAssignmentReceipt,
};
use harness_contract::reality::EvidenceRef;
use harness_contract::task::{TaskCreateCommand, TaskPhaseSpec, TaskStatus, TaskTurnBinding};

use crate::{
    MissionRuntime, RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeServices,
    RuntimeTransactionEventInput, TaskAggregate, TaskMutationResult,
};

/// Runtime's sole surface-facing Task boundary.
///
/// The aggregate and evidence outbox are committed atomically by the selected
/// Task backend. This port projects pending evidence into Runtime's event log,
/// validates Mission availability, and resolves graph references from the
/// canonical graph store instead of caching graph projections in Task.
#[derive(Clone)]
pub struct TaskRuntimePort {
    tasks: Arc<crate::TaskAggregateService>,
    missions: Arc<MissionRuntime>,
    events: Arc<crate::RuntimeEventStore>,
    graphs: crate::ExecutionGraphStateStore,
}

impl TaskRuntimePort {
    #[must_use]
    pub fn new(services: &RuntimeServices) -> Self {
        Self::from_components(
            Arc::clone(services.task_aggregate_service()),
            Arc::clone(services.mission_runtime()),
            Arc::clone(services.event_store()),
            services.graph_state_store().clone(),
        )
    }

    pub(crate) fn from_components(
        tasks: Arc<crate::TaskAggregateService>,
        missions: Arc<MissionRuntime>,
        events: Arc<crate::RuntimeEventStore>,
        graphs: crate::ExecutionGraphStateStore,
    ) -> Self {
        Self {
            tasks,
            missions,
            events,
            graphs,
        }
    }

    pub fn list(&self) -> Result<Vec<TaskAggregate>, String> {
        Ok(self
            .tasks
            .list()?
            .into_iter()
            .filter(|task| self.mission_is_visible(&task.mission_id))
            .collect())
    }

    pub fn get(&self, task_id: &str) -> Result<Option<TaskAggregate>, String> {
        Ok(self
            .tasks
            .get(task_id)?
            .filter(|task| self.mission_is_visible(&task.mission_id)))
    }

    pub fn open_root_candidates(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<TaskAggregate>, String> {
        self.tasks.open_root_candidates(session_id, limit)
    }

    pub fn organization_candidates(&self, limit: usize) -> Result<Vec<TaskAggregate>, String> {
        self.tasks.organization_candidates(limit)
    }

    pub fn unorganized_candidates(&self, limit: usize) -> Result<Vec<TaskAggregate>, String> {
        self.tasks.unorganized_candidates(limit)
    }

    pub fn bindings_for_task(&self, task_id: &str) -> Result<Vec<TaskTurnBinding>, String> {
        self.require_visible_task(task_id)?;
        self.tasks.bindings_for_task(task_id)
    }

    pub fn bindings_for_turn(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> Result<Vec<TaskTurnBinding>, String> {
        self.tasks.bindings_for_turn(session_id, turn_id)
    }

    pub fn mission_workspace_id(&self, mission_id: &str) -> Result<String, String> {
        self.missions
            .aggregate(mission_id)
            .map(|mission| mission.workspace_id)
            .ok_or_else(|| format!("mission `{mission_id}` not found"))
    }

    #[must_use]
    pub fn workspace_default_mission_id(&self) -> &str {
        self.missions.default_mission_id()
    }

    pub fn latest_terminal_runtime_receipt(
        &self,
        task_id: &str,
    ) -> Result<Option<crate::DurableRuntimeEvent>, String> {
        if task_id.trim().is_empty() || self.get(task_id)?.is_none() {
            return Ok(None);
        }
        self.events
            .list_stream(&format!("task:{task_id}"))
            .map(|events| {
                events.into_iter().rev().find(|event| {
                    event.status.as_deref().is_some_and(|status| {
                        matches!(status, "completed" | "cancelled" | "failed" | "blocked")
                    })
                })
            })
            .map_err(|error| error.to_string())
    }

    pub fn record_assignment_terminal_observation(
        &self,
        task_id: &str,
        terminal_status: &str,
        source_receipt_ref: &str,
        correlation_id: &str,
    ) -> Result<crate::DurableRuntimeEvent, String> {
        self.require_visible_task(task_id)?;
        record_task_terminal_observation(
            &self.events,
            task_id,
            terminal_status,
            source_receipt_ref,
            correlation_id,
        )
    }

    pub fn create(&self, command: TaskCreateCommand) -> Result<crate::TaskCommandOutcome, String> {
        let mission_id = command.mission_id.clone();
        let evidence_refs = command.evidence_refs.clone();
        self.ensure_mission_active(&mission_id, &evidence_refs)?;
        let result = self.tasks.create(command)?;
        self.finish_mutation(result)
    }

    pub fn preview_mission_assignment(
        &self,
        command: TaskMissionAssignmentCommand,
    ) -> Result<(TaskMissionAssignmentCommand, TaskMissionAssignmentPreview), String> {
        self.ensure_mission_active(&command.target_mission_id, &command.evidence_refs)?;
        self.tasks.preview_mission_assignment(command)
    }

    pub fn assign_mission_batch(
        &self,
        command: &TaskMissionAssignmentCommand,
    ) -> Result<TaskMissionAssignmentReceipt, String> {
        self.ensure_mission_active(&command.target_mission_id, &command.evidence_refs)?;
        let receipt = self.tasks.assign_mission_batch(command)?;
        for task_id in receipt.task_revisions.keys() {
            self.project_task_evidence(task_id, 256)?;
        }
        Ok(receipt)
    }

    pub fn assignment_receipt(
        &self,
        operation_id: &str,
    ) -> Result<Option<TaskMissionAssignmentReceipt>, String> {
        self.tasks.assignment_receipt(operation_id)
    }

    pub fn save_organization_decision(
        &self,
        decision: &MissionOrganizationDecision,
        expected_revision: Option<u64>,
    ) -> Result<MissionOrganizationDecision, String> {
        self.tasks
            .save_organization_decision(decision, expected_revision)
    }

    pub fn organization_decisions(
        &self,
        status: Option<MissionOrganizationStatus>,
        limit: usize,
    ) -> Result<Vec<MissionOrganizationDecision>, String> {
        self.tasks.organization_decisions(status, limit)
    }

    pub fn start_phase(
        &self,
        task_id: &str,
        expected_revision: u64,
        spec: TaskPhaseSpec,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<crate::TaskCommandOutcome, String> {
        self.require_visible_task(task_id)?;
        let result = self
            .tasks
            .start_phase(task_id, expected_revision, spec, evidence_refs)?;
        self.finish_mutation(result)
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
    ) -> Result<crate::TaskCommandOutcome, String> {
        self.require_visible_task(task_id)?;
        let result = self.tasks.record_phase_artifact(
            task_id,
            expected_revision,
            phase_id,
            kind,
            label,
            value,
            evidence_refs,
        )?;
        self.finish_mutation(result)
    }

    pub fn review_phase(
        &self,
        task_id: &str,
        expected_revision: u64,
        phase_id: &str,
        result: impl Into<String>,
        completed: bool,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<crate::TaskCommandOutcome, String> {
        self.require_visible_task(task_id)?;
        let result = self.tasks.review_phase(
            task_id,
            expected_revision,
            phase_id,
            result,
            completed,
            evidence_refs,
        )?;
        self.finish_mutation(result)
    }

    pub fn transition(
        &self,
        task_id: &str,
        expected_revision: u64,
        status: TaskStatus,
        evidence_refs: Vec<EvidenceRef>,
        note: impl Into<String>,
    ) -> Result<crate::TaskCommandOutcome, String> {
        self.require_visible_task(task_id)?;
        let result =
            self.tasks
                .transition(task_id, expected_revision, status, evidence_refs, note)?;
        self.finish_mutation(result)
    }

    pub fn record_failure(
        &self,
        task_id: &str,
        expected_revision: u64,
        reason: impl Into<String>,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<crate::TaskCommandOutcome, String> {
        self.require_visible_task(task_id)?;
        let result =
            self.tasks
                .record_failure(task_id, expected_revision, reason, evidence_refs)?;
        self.finish_mutation(result)
    }

    pub async fn execution_graph(
        &self,
        task_id: &str,
    ) -> Result<Option<ExecutionGraphProjection>, String> {
        let Some(task) = self.get(task_id)? else {
            return Ok(None);
        };
        let Some(reference) = task.graph_refs.last() else {
            return Ok(None);
        };
        self.graphs
            .projection_async(reference.graph_id.clone())
            .await
            .map(Some)
            .map_err(|error| error.to_string())
    }

    pub async fn execution_graphs(&self) -> Result<Vec<ExecutionGraphProjection>, String> {
        let mut seen = std::collections::BTreeSet::new();
        let mut projections = Vec::new();
        for task in self.list()? {
            for reference in task.graph_refs {
                if !seen.insert(reference.graph_id.clone()) {
                    continue;
                }
                projections.push(
                    self.graphs
                        .projection_async(reference.graph_id)
                        .await
                        .map_err(|error| error.to_string())?,
                );
            }
        }
        Ok(projections)
    }

    pub fn link_existing_graph(
        &self,
        task_id: &str,
        graph_id: &str,
        graph_revision: u64,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<crate::TaskCommandOutcome, String> {
        let task = self
            .get(task_id)?
            .ok_or_else(|| format!("task `{task_id}` not found"))?;
        let result = self.tasks.link_graph(
            task_id,
            task.revision,
            graph_id,
            graph_revision,
            evidence_refs.clone(),
        )?;
        self.finish_mutation(result)
    }

    /// Replay all unprojected Task evidence. Startup recovery and post-mutation
    /// callers use the same idempotent path.
    pub fn project_pending_evidence(&self, limit: usize) -> Result<usize, String> {
        let mut records = Vec::new();
        for task in self.list()? {
            let remaining = limit.saturating_sub(records.len());
            if remaining == 0 {
                break;
            }
            records.extend(self.tasks.pending_outbox(Some(&task.task_id), remaining)?);
        }
        records.sort_by(|left, right| {
            left.created_at_ms
                .cmp(&right.created_at_ms)
                .then_with(|| left.outbox_id.cmp(&right.outbox_id))
        });
        records.truncate(limit);
        self.project_evidence_records(records)
    }

    fn project_task_evidence(&self, task_id: &str, limit: usize) -> Result<usize, String> {
        self.require_visible_task(task_id)?;
        let records = self.tasks.pending_outbox(Some(task_id), limit)?;
        self.project_evidence_records(records)
    }

    fn project_evidence_records(
        &self,
        records: Vec<crate::TaskEvidenceOutboxRecord>,
    ) -> Result<usize, String> {
        let mut projected = 0;
        for record in records {
            let stream_id = format!("task:{}", record.task_id);
            if self
                .events
                .event_by_idempotency_key(&stream_id, &record.outbox_id)
                .map_err(|error| error.to_string())?
                .is_none()
            {
                let mut refs = vec![RuntimeEventRef {
                    kind: "task".to_string(),
                    id: record.task_id.clone(),
                }];
                if let Some(task) = self.tasks.get(&record.task_id)? {
                    refs.extend([
                        RuntimeEventRef {
                            kind: "mission".to_string(),
                            id: task.mission_id,
                        },
                        RuntimeEventRef {
                            kind: "session".to_string(),
                            id: task.origin_session_id,
                        },
                        RuntimeEventRef {
                            kind: "turn".to_string(),
                            id: task.origin_turn_id,
                        },
                    ]);
                    refs.extend(
                        task.graph_refs
                            .into_iter()
                            .map(|reference| RuntimeEventRef {
                                kind: "execution_graph".to_string(),
                                id: reference.graph_id,
                            }),
                    );
                }
                refs.extend(
                    record
                        .evidence_refs
                        .iter()
                        .map(|reference| RuntimeEventRef {
                            kind: reference.ref_type.clone(),
                            id: reference.id.clone(),
                        }),
                );
                refs.sort_by(|left, right| {
                    left.kind
                        .cmp(&right.kind)
                        .then_with(|| left.id.cmp(&right.id))
                });
                refs.dedup_by(|left, right| left.kind == right.kind && left.id == right.id);
                let event = RuntimeEventInput {
                    stream_id: stream_id.clone(),
                    scope: RuntimeEventScope::Task,
                    kind: record.event_kind.clone(),
                    status: Some(record.status.as_str().to_string()),
                    actor: Some("runtime.task".to_string()),
                    refs,
                    payload: serde_json::to_value(&record).map_err(|error| error.to_string())?,
                };
                let mut committed = false;
                for _ in 0..4 {
                    let expected = self
                        .events
                        .stream_revision(&stream_id)
                        .map_err(|error| error.to_string())?;
                    match self.events.append_batch_if_revision(
                        stream_id.clone(),
                        expected,
                        format!("task-outbox:{}", record.outbox_id),
                        vec![RuntimeTransactionEventInput {
                            event: event.clone(),
                            idempotency_key: Some(record.outbox_id.clone()),
                            schema_version: 1,
                        }],
                    ) {
                        Ok(_) => {
                            committed = true;
                            break;
                        }
                        Err(_) => {
                            if self
                                .events
                                .event_by_idempotency_key(&stream_id, &record.outbox_id)
                                .map_err(|error| error.to_string())?
                                .is_some()
                            {
                                committed = true;
                                break;
                            }
                        }
                    }
                }
                if !committed {
                    return Err(format!(
                        "task evidence outbox `{}` could not be projected",
                        record.outbox_id
                    ));
                }
            }
            self.tasks
                .mark_outbox_projected(&record.outbox_id, now_ms())?;
            projected += 1;
        }
        Ok(projected)
    }

    /// Restore every committed Task side effect before Runtime begins serving.
    ///
    /// Recovery is deliberately outside read paths: queries remain bounded by
    /// requested data while startup drains the durable evidence outbox.
    pub fn recover(&self) -> Result<(), String> {
        const RECOVERY_BATCH_SIZE: usize = 256;
        loop {
            let projected = self.project_pending_evidence(RECOVERY_BATCH_SIZE)?;
            if projected < RECOVERY_BATCH_SIZE {
                break;
            }
        }
        Ok(())
    }

    fn finish_mutation(
        &self,
        result: TaskMutationResult,
    ) -> Result<crate::TaskCommandOutcome, String> {
        if result.receipt.task_id != result.aggregate.task_id
            || result.receipt.accepted_revision != result.aggregate.revision
            || result.receipt.status != result.aggregate.status
            || result.outbox.as_ref().is_some_and(|outbox| {
                result.receipt.outbox_id != outbox.outbox_id
                    || result.receipt.evidence_refs != outbox.evidence_refs
            })
        {
            return Err(format!(
                "task `{}` backend returned a command receipt inconsistent with its committed aggregate/outbox",
                result.aggregate.task_id
            ));
        }
        self.project_task_evidence(&result.aggregate.task_id, 256)?;
        Ok(crate::TaskCommandOutcome {
            aggregate: result.aggregate,
            command_receipt: result.receipt,
        })
    }

    fn ensure_mission_active(
        &self,
        mission_id: &str,
        evidence_refs: &[EvidenceRef],
    ) -> Result<(), String> {
        let mission = match self.missions.aggregate(mission_id) {
            Some(mission) => mission,
            None if mission_id == self.missions.default_mission_id() => {
                self.missions.ensure_default_mission()?
            }
            None => return Err(format!("mission `{mission_id}` not found")),
        };
        if mission.status == MissionStatus::Draft {
            self.missions
                .activate_if_draft(mission_id, evidence_refs.to_vec())?;
        }
        Ok(())
    }

    fn mission_is_visible(&self, mission_id: &str) -> bool {
        mission_id == self.missions.default_mission_id()
            || self.missions.aggregate(mission_id).is_some()
    }

    fn require_visible_task(&self, task_id: &str) -> Result<TaskAggregate, String> {
        self.get(task_id)?
            .ok_or_else(|| format!("task `{task_id}` not found"))
    }
}

fn record_task_terminal_observation(
    event_store: &crate::RuntimeEventStore,
    task_id: &str,
    terminal_status: &str,
    source_receipt_ref: &str,
    correlation_id: &str,
) -> Result<crate::DurableRuntimeEvent, String> {
    if !matches!(
        terminal_status,
        "completed" | "cancelled" | "failed" | "blocked"
    ) {
        return Err(format!(
            "task terminal observation rejected non-terminal status {terminal_status}"
        ));
    }
    for (field, value) in [
        ("task_id", task_id),
        ("source_receipt_ref", source_receipt_ref),
        ("correlation_id", correlation_id),
    ] {
        if value.trim().is_empty() {
            return Err(format!("task terminal observation requires {field}"));
        }
    }
    let stream_id = format!("task-observation:{task_id}");
    let idempotency_key = format!("assignment-terminal-observed:{correlation_id}");
    if let Some(existing) = event_store
        .event_by_idempotency_key(&stream_id, &idempotency_key)
        .map_err(|error| error.to_string())?
    {
        return Ok(existing);
    }
    let expected_revision = event_store
        .stream_revision(&stream_id)
        .map_err(|error| error.to_string())?;
    if let Err(error) = event_store.append_batch_if_revision(
        stream_id.clone(),
        expected_revision,
        format!("runtime-task-observation:{task_id}:{correlation_id}"),
        vec![RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id: stream_id.clone(),
                scope: RuntimeEventScope::Relation,
                kind: "application.assignment.task_terminal_observed.v1".to_string(),
                status: Some(terminal_status.to_string()),
                actor: Some("runtime.application_projection".to_string()),
                refs: vec![
                    RuntimeEventRef {
                        kind: "task".to_string(),
                        id: task_id.to_string(),
                    },
                    RuntimeEventRef {
                        kind: "source_receipt".to_string(),
                        id: source_receipt_ref.to_string(),
                    },
                ],
                payload: serde_json::json!({
                    "task_id": task_id,
                    "status": terminal_status,
                    "source_receipt_ref": source_receipt_ref,
                    "correlation_id": correlation_id,
                    "observed_for": "application.assignment.complete",
                }),
            },
            idempotency_key: Some(idempotency_key.clone()),
            schema_version: 1,
        }],
    ) {
        if let Some(existing) = event_store
            .event_by_idempotency_key(&stream_id, &idempotency_key)
            .map_err(|lookup_error| lookup_error.to_string())?
        {
            return Ok(existing);
        }
        return Err(error.to_string());
    }
    event_store
        .event_by_idempotency_key(&stream_id, &idempotency_key)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "task terminal observation committed without a readable event".to_string())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
