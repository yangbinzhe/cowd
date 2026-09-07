//! Runtime-owned Task aggregate, persistence contract, and evidence outbox.

use std::{
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
use serde::{Deserialize, Serialize};

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
}

#[derive(Debug, Default)]
struct EphemeralTaskState {
    tasks: std::collections::BTreeMap<String, TaskAggregate>,
    bindings: Vec<TaskTurnBinding>,
    evidence_outbox: std::collections::BTreeMap<String, TaskEvidenceOutboxRecord>,
    assignment_outbox: std::collections::BTreeMap<String, TaskMissionAssignmentOutboxRecord>,
    decisions: std::collections::BTreeMap<String, MissionOrganizationDecision>,
}

#[derive(Debug, Default)]
pub struct EphemeralTaskStore {
    state: std::sync::Mutex<EphemeralTaskState>,
}

impl EphemeralTaskStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn mutation_result(
        state: &mut EphemeralTaskState,
        aggregate: TaskAggregate,
        mutation: &TaskMutation,
    ) -> TaskMutationResult {
        let changed = aggregate.revision != mutation.expected_revision;
        let outbox = changed.then(|| TaskEvidenceOutboxRecord {
            outbox_id: format!("task-outbox:{}:{}", aggregate.task_id, aggregate.revision),
            task_id: aggregate.task_id.clone(),
            revision: aggregate.revision,
            event_kind: mutation.event_kind.clone(),
            status: aggregate.status,
            evidence_refs: mutation.evidence_refs.clone(),
            created_at_ms: aggregate.updated_at_ms,
            projected_at_ms: None,
        });
        if let Some(record) = &outbox {
            state
                .evidence_outbox
                .insert(record.outbox_id.clone(), record.clone());
        }
        state
            .tasks
            .insert(aggregate.task_id.clone(), aggregate.clone());
        TaskMutationResult::from_backend_commit(aggregate, mutation, outbox)
    }
}

impl TaskStoreBackend for EphemeralTaskStore {
    fn list(&self) -> Result<Vec<TaskAggregate>, String> {
        Ok(self.state.lock().unwrap().tasks.values().cloned().collect())
    }
    fn bind_turn(&self, binding: &TaskTurnBinding) -> Result<TaskTurnBinding, String> {
        binding.validate()?;
        let mut state = self.state.lock().unwrap();
        if let Some(existing) = state.bindings.iter().find(|v| {
            v.task_id == binding.task_id
                && v.session_id == binding.session_id
                && v.turn_id == binding.turn_id
                && v.role == binding.role
        }) {
            return Ok(existing.clone());
        }
        state.bindings.push(binding.clone());
        Ok(binding.clone())
    }
    fn create_with_origin_binding(
        &self,
        aggregate: &TaskAggregate,
        mutation: &TaskMutation,
        binding: &TaskTurnBinding,
    ) -> Result<(TaskMutationResult, TaskTurnBinding), String> {
        validate_task_aggregate_for_backend(aggregate)?;
        binding.validate()?;
        let mut state = self.state.lock().unwrap();
        if let Some(existing) = state.tasks.get(&aggregate.task_id).cloned() {
            let result = TaskMutationResult::from_backend_commit(existing, mutation, None);
            return Ok((result, binding.clone()));
        }
        let result = Self::mutation_result(&mut state, aggregate.clone(), mutation);
        state.bindings.push(binding.clone());
        Ok((result, binding.clone()))
    }
    fn bindings_for_task(&self, id: &str) -> Result<Vec<TaskTurnBinding>, String> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .bindings
            .iter()
            .filter(|v| v.task_id == id)
            .cloned()
            .collect())
    }
    fn bindings_for_turn(&self, session: &str, turn: &str) -> Result<Vec<TaskTurnBinding>, String> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .bindings
            .iter()
            .filter(|v| v.session_id == session && v.turn_id == turn)
            .cloned()
            .collect())
    }
    fn assign_mission_batch(
        &self,
        command: &TaskMissionAssignmentCommand,
    ) -> Result<TaskMissionAssignmentReceipt, String> {
        validate_assignment_command(command)?;
        let mut state = self.state.lock().unwrap();
        if let Some(existing) = state.assignment_outbox.get(&command.operation_id) {
            validate_assignment_replay(command, &existing.receipt)?;
            return Ok(existing.receipt.clone());
        }
        let applied = now_ms();
        let mut revisions = std::collections::BTreeMap::new();
        for id in &command.task_ids {
            let task = state
                .tasks
                .get(id)
                .ok_or_else(|| format!("task `{id}` not found"))?;
            let expected = command
                .expected_task_revisions
                .get(id)
                .ok_or_else(|| format!("task `{id}` has no expected revision"))?;
            if task.revision != *expected {
                return Err(format!(
                    "task `{id}` revision conflict: expected {expected}, actual {}",
                    task.revision
                ));
            }
        }
        for id in &command.task_ids {
            let task = state.tasks.get_mut(id).unwrap();
            task.mission_id.clone_from(&command.target_mission_id);
            task.mission_assignment = command.assignment;
            task.mission_assignment_revision += 1;
            task.mission_assigned_by.clone_from(&command.actor);
            task.mission_assignment_evidence_refs = command.evidence_refs.clone();
            task.revision += 1;
            task.updated_at_ms = applied;
            revisions.insert(id.clone(), task.revision);
        }
        let receipt = TaskMissionAssignmentReceipt {
            operation_id: command.operation_id.clone(),
            target_mission_id: command.target_mission_id.clone(),
            task_revisions: revisions,
            assignment: command.assignment,
            applied_at_ms: applied,
            evidence_refs: command.evidence_refs.clone(),
        };
        state.assignment_outbox.insert(
            command.operation_id.clone(),
            TaskMissionAssignmentOutboxRecord {
                operation_id: command.operation_id.clone(),
                receipt: receipt.clone(),
                created_at_ms: applied,
                projected_at_ms: None,
            },
        );
        Ok(receipt)
    }
    fn assignment_receipt(&self, id: &str) -> Result<Option<TaskMissionAssignmentReceipt>, String> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .assignment_outbox
            .get(id)
            .map(|v| v.receipt.clone()))
    }
    fn save_organization_decision(
        &self,
        decision: &MissionOrganizationDecision,
        expected: Option<u64>,
    ) -> Result<MissionOrganizationDecision, String> {
        validate_organization_decision(decision)?;
        let mut state = self.state.lock().unwrap();
        match (state.decisions.get(&decision.decision_id), expected) {
            (None, None) => {}
            (Some(current), Some(rev)) if current.revision == rev => {}
            (Some(current), None) => return Ok(current.clone()),
            (Some(current), _) => {
                return Err(format!(
                    "organization decision `{}` revision conflict at {}",
                    decision.decision_id, current.revision
                ))
            }
            (None, Some(_)) => {
                return Err(format!(
                    "organization decision `{}` does not exist",
                    decision.decision_id
                ))
            }
        }
        state
            .decisions
            .insert(decision.decision_id.clone(), decision.clone());
        Ok(decision.clone())
    }
    fn organization_decisions(
        &self,
        status: Option<MissionOrganizationStatus>,
        limit: usize,
    ) -> Result<Vec<MissionOrganizationDecision>, String> {
        let mut values = self
            .state
            .lock()
            .unwrap()
            .decisions
            .values()
            .filter(|v| status.is_none_or(|s| v.status == s))
            .cloned()
            .collect::<Vec<_>>();
        values.sort_by_key(|v| std::cmp::Reverse(v.updated_at_ms));
        values.truncate(limit);
        Ok(values)
    }
    fn mutate_task(
        &self,
        id: &str,
        mutation: &TaskMutation,
        updater: &mut dyn FnMut(Option<TaskAggregate>) -> Result<TaskAggregate, String>,
    ) -> Result<TaskMutationResult, String> {
        let mut state = self.state.lock().unwrap();
        let current = state.tasks.get(id).cloned();
        let actual = current.as_ref().map_or(0, |v| v.revision);
        if actual != mutation.expected_revision {
            return Err(format!(
                "task `{id}` revision conflict: expected {}, actual {actual}",
                mutation.expected_revision
            ));
        }
        let next = updater(current.clone())?;
        validate_backend_mutation(id, current.as_ref(), &next, mutation)?;
        Ok(Self::mutation_result(&mut state, next, mutation))
    }
    fn pending_outbox(
        &self,
        task: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TaskEvidenceOutboxRecord>, String> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .evidence_outbox
            .values()
            .filter(|v| v.projected_at_ms.is_none() && task.is_none_or(|id| v.task_id == id))
            .take(limit)
            .cloned()
            .collect())
    }
    fn list_outbox(&self) -> Result<Vec<TaskEvidenceOutboxRecord>, String> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .evidence_outbox
            .values()
            .cloned()
            .collect())
    }
    fn list_assignment_outbox(&self) -> Result<Vec<TaskMissionAssignmentOutboxRecord>, String> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .assignment_outbox
            .values()
            .cloned()
            .collect())
    }
    fn mark_outbox_projected(&self, id: &str, at: u64) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        if let Some(v) = state.evidence_outbox.get_mut(id) {
            v.projected_at_ms = Some(at);
            return Ok(());
        }
        if let Some(v) = state.assignment_outbox.get_mut(id) {
            v.projected_at_ms = Some(at);
            return Ok(());
        }
        Err(format!("task outbox `{id}` not found"))
    }
}

#[derive(Debug, Clone)]
pub struct TaskAggregateService {
    backend: Arc<dyn TaskStoreBackend>,
}

impl TaskAggregateService {
    #[must_use]
    pub fn for_test() -> Self {
        Self::from_backend(Arc::new(EphemeralTaskStore::new()))
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

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn validate_required(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        Err(format!("{field} must not be empty"))
    } else {
        Ok(())
    }
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
        application_provenance: command.spec.application_provenance,
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
        && left.application_provenance == right.application_provenance
}

#[must_use]
pub fn synthetic_evidence(ref_type: &str, id: impl Into<String>) -> EvidenceRef {
    EvidenceRef::observed(ref_type, id).with_source("runtime.task")
}
