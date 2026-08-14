use std::collections::BTreeMap;
use std::sync::Arc;

use harness_contract::{
    execution_graph::ExecutionGraphProjection,
    mission::{
        MissionOrganizationDecision, MissionOrganizationStatus, TaskMissionAssignmentCommand,
        TaskMissionAssignmentPreview, TaskMissionAssignmentReceipt,
    },
    reality::EvidenceRef,
    task::{
        TaskApplicationProvenance, TaskCreateCommand, TaskKind, TaskMissionAssignment, TaskOrigin,
        TaskPhaseSpec, TaskSpec, TaskStatus,
    },
};

use super::ServiceEnvelope;

/// Gateway adapter for Runtime-owned Task commands and projections.
///
/// This service owns no Task state, graph cache, lifecycle policy, or evidence
/// writer. It translates surface commands into the one Runtime Task port.
#[derive(Clone)]
pub(crate) struct TaskService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    port: Option<runtime::TaskRuntimePort>,
}

impl TaskService {
    pub(crate) fn new() -> Self {
        Self {
            label: "task",
            owner: "Runtime Task aggregate port",
            port: None,
        }
    }

    pub(crate) fn with_runtime(runtime_services: Arc<runtime::RuntimeServices>) -> Self {
        Self {
            port: Some(runtime_services.task_runtime_port()),
            ..Self::new()
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        ServiceEnvelope {
            service: self.label,
            operation,
            status: if self.port.is_some() {
                "service_ready"
            } else {
                "service_boundary_ready"
            },
            owner: self.owner,
            boundary_status: "runtime_task_authority",
        }
    }

    fn port(&self) -> Result<&runtime::TaskRuntimePort, String> {
        self.port
            .as_ref()
            .ok_or_else(|| "Runtime Task port is not configured".to_string())
    }

    pub(crate) fn list_records(&self) -> Result<Vec<runtime::TaskAggregate>, String> {
        self.port()?.list()
    }

    pub(crate) fn get(&self, task_id: &str) -> Result<Option<runtime::TaskAggregate>, String> {
        self.port()?.get(task_id)
    }

    pub(crate) fn bindings_for_task(
        &self,
        task_id: &str,
    ) -> Result<Vec<harness_contract::task::TaskTurnBinding>, String> {
        self.port()?.bindings_for_task(task_id)
    }

    pub(crate) fn preview_mission_assignment(
        &self,
        command: TaskMissionAssignmentCommand,
    ) -> Result<(TaskMissionAssignmentCommand, TaskMissionAssignmentPreview), String> {
        self.port()?.preview_mission_assignment(command)
    }

    pub(crate) fn assignment_command(
        &self,
        operation_id: String,
        task_ids: Vec<String>,
        target_mission_id: String,
        assignment: TaskMissionAssignment,
        actor: String,
        expected_task_revisions: BTreeMap<String, u64>,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<TaskMissionAssignmentCommand, String> {
        let workspace_id = self.port()?.mission_workspace_id(&target_mission_id)?;
        Ok(TaskMissionAssignmentCommand {
            operation_id,
            workspace_id,
            task_ids,
            target_mission_id,
            assignment,
            actor,
            expected_task_revisions,
            evidence_refs,
        })
    }

    pub(crate) fn assign_mission_batch(
        &self,
        command: &TaskMissionAssignmentCommand,
    ) -> Result<TaskMissionAssignmentReceipt, String> {
        self.port()?.assign_mission_batch(command)
    }

    pub(crate) fn assignment_receipt(
        &self,
        operation_id: &str,
    ) -> Result<Option<TaskMissionAssignmentReceipt>, String> {
        self.port()?.assignment_receipt(operation_id)
    }

    pub(crate) fn organization_decisions(
        &self,
        status: Option<MissionOrganizationStatus>,
        limit: usize,
    ) -> Result<Vec<MissionOrganizationDecision>, String> {
        self.port()?.organization_decisions(status, limit)
    }

    pub(crate) fn workspace_default_mission_id(&self) -> Result<String, String> {
        Ok(self.port()?.workspace_default_mission_id().to_string())
    }

    pub(crate) fn create(
        &self,
        task_id: String,
        mission_id: String,
        origin_session_id: String,
        origin_turn_id: String,
        objective: String,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<runtime::TaskCommandOutcome, String> {
        let spec =
            self.port()?
                .bind_task_spec(&origin_session_id, None, TaskSpec::new(objective))?;
        self.port()?.create(TaskCreateCommand {
            root_task_id: task_id.clone(),
            task_id,
            mission_id,
            kind: TaskKind::Root,
            origin: TaskOrigin::User,
            origin_session_id,
            origin_turn_id,
            parent_task_id: None,
            predecessor_task_id: None,
            mission_assignment: TaskMissionAssignment::ExplicitLocked,
            mission_assigned_by: "gateway.task_api".to_string(),
            spec,
            evidence_refs,
        })
    }

    /// Create the canonical Runtime task for one application-structured
    /// request. Gateway is the only caller allowed to materialize the
    /// provenance; the application payload never crosses this boundary as a
    /// `TaskSpec`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_application_structured(
        &self,
        task_id: String,
        mission_id: String,
        origin_session_id: String,
        origin_turn_id: String,
        objective: String,
        provenance: TaskApplicationProvenance,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<runtime::TaskCommandOutcome, String> {
        provenance.validate()?;
        let mut spec = TaskSpec::new(objective);
        spec.application_provenance = Some(provenance);
        let spec = self
            .port()?
            .bind_task_spec(&origin_session_id, None, spec)?;
        self.port()?.create(TaskCreateCommand {
            root_task_id: task_id.clone(),
            task_id,
            mission_id,
            kind: TaskKind::Root,
            origin: TaskOrigin::System,
            origin_session_id,
            origin_turn_id,
            parent_task_id: None,
            predecessor_task_id: None,
            mission_assignment: TaskMissionAssignment::ExplicitLocked,
            mission_assigned_by: "gateway.application_structured_task".to_string(),
            spec,
            evidence_refs,
        })
    }

    pub(crate) fn start_phase(
        &self,
        task_id: &str,
        expected_revision: u64,
        spec: TaskPhaseSpec,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<runtime::TaskCommandOutcome, String> {
        self.port()?
            .start_phase(task_id, expected_revision, spec, evidence_refs)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_phase_artifact(
        &self,
        task_id: &str,
        expected_revision: u64,
        phase_id: &str,
        kind: String,
        label: String,
        value: String,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<runtime::TaskCommandOutcome, String> {
        self.port()?.record_phase_artifact(
            task_id,
            expected_revision,
            phase_id,
            kind,
            label,
            value,
            evidence_refs,
        )
    }

    pub(crate) fn review_phase(
        &self,
        task_id: &str,
        expected_revision: u64,
        phase_id: &str,
        result: String,
        completed: bool,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<runtime::TaskCommandOutcome, String> {
        self.port()?.review_phase(
            task_id,
            expected_revision,
            phase_id,
            result,
            completed,
            evidence_refs,
        )
    }

    pub(crate) fn transition(
        &self,
        task_id: &str,
        expected_revision: u64,
        status: TaskStatus,
        evidence_refs: Vec<EvidenceRef>,
        note: String,
    ) -> Result<runtime::TaskCommandOutcome, String> {
        self.port()?
            .transition(task_id, expected_revision, status, evidence_refs, note)
    }

    pub(crate) fn record_failure(
        &self,
        task_id: &str,
        expected_revision: u64,
        reason: String,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<runtime::TaskCommandOutcome, String> {
        self.port()?
            .record_failure(task_id, expected_revision, reason, evidence_refs)
    }

    pub(crate) async fn execution_graphs(&self) -> Result<Vec<ExecutionGraphProjection>, String> {
        self.port()?.execution_graphs().await
    }

    pub(crate) async fn execution_graph(
        &self,
        task_id: &str,
    ) -> Result<Option<ExecutionGraphProjection>, String> {
        self.port()?.execution_graph(task_id).await
    }

    pub(crate) fn latest_terminal_runtime_receipt(
        &self,
        task_id: &str,
    ) -> Result<Option<runtime::DurableRuntimeEvent>, String> {
        self.port()?.latest_terminal_runtime_receipt(task_id)
    }

    pub(crate) fn record_assignment_terminal_observation(
        &self,
        task_id: &str,
        terminal_status: &str,
        source_receipt_ref: &str,
        correlation_id: &str,
    ) -> Result<runtime::DurableRuntimeEvent, String> {
        self.port()?.record_assignment_terminal_observation(
            task_id,
            terminal_status,
            source_receipt_ref,
            correlation_id,
        )
    }
}
