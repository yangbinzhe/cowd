use std::sync::Arc;

use harness_contract::{
    execution_graph::{ExecutionEdge, ExecutionGraphProjection, ExecutionNodeSpec},
    reality::EvidenceRef,
    task::{TaskCreateCommand, TaskPhaseSpec, TaskSpec, TaskStatus},
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

    pub(crate) fn current(&self) -> Result<Option<runtime::TaskAggregate>, String> {
        self.port()?.current()
    }

    pub(crate) fn mission_id_for_session(&self, session_id: &str) -> Result<String, String> {
        Ok(self.port()?.mission_id_for_session(session_id))
    }

    pub(crate) fn workspace_default_mission_id(&self) -> Result<String, String> {
        Ok(self.port()?.workspace_default_mission_id().to_string())
    }

    pub(crate) fn create(
        &self,
        task_id: String,
        mission_id: String,
        source_session_id: String,
        source_turn_id: String,
        objective: String,
        yolo_mode: bool,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<runtime::TaskCommandOutcome, String> {
        let mut spec = TaskSpec::new(objective);
        spec.execution_policy.yolo_mode = yolo_mode;
        self.port()?.create(TaskCreateCommand {
            task_id,
            mission_id,
            source_session_id,
            source_turn_id,
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

    pub(crate) async fn register_execution_graph(
        &self,
        task_id: &str,
        objective: Option<String>,
        nodes: Vec<ExecutionNodeSpec>,
        edges: Vec<ExecutionEdge>,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<ExecutionGraphProjection, String> {
        self.port()?
            .register_execution_graph(task_id, objective, nodes, edges, evidence_refs)
            .await
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
