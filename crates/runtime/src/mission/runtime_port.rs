//! Narrow Runtime-owned Mission query and command port.
//!
//! Gateway, TUI, and other surfaces must not assemble Mission projections or
//! mutate Mission-adjacent aggregates through `RuntimeServices` directly.
//! This port keeps those aggregate joins inside Runtime while preserving the
//! existing event-sourced stores as the single authority.

use std::sync::Arc;

use harness_contract::execution_graph::{
    validate_execution_graph, ExecutionEdge, ExecutionGraph, ExecutionGraphCommand,
    ExecutionGraphProjection, ExecutionNodeSpec,
};
use harness_contract::mission::{
    MissionCommand, MissionCommandSagaRecord, MissionControlProjection, MissionControlSessionNode,
    MissionStatus,
};
use harness_contract::reality::EvidenceRef;
use harness_contract::task::{TaskCreateCommand, TaskPhaseSpec, TaskStatus};
use harness_contract::team::TeamInstantiationRequest;
use serde_json::{json, Value};

use crate::{
    ApprovalDecisionCommand, CreateMissionScheduleRequest, ExecutionGraphHost,
    MissionCommandInterpretation, MissionCommandInterpreter, MissionControlRuntime,
    MissionProjection, MissionRuntime, MissionScheduleDispatchReport, MissionSchedulePolicy,
    RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeServices,
    RuntimeTransactionEventInput, SessionProxy, SessionRelationGraph, SubmitGlobalApprovalRequest,
    TaskAggregate, TaskMutationResult, TeamProjection, UpdateMissionScheduleRequest,
};

/// Runtime's sole surface-facing Mission boundary.
///
/// It owns no independent state. Every mutation delegates to a canonical
/// Runtime aggregate and every read rebuilds from its Runtime projection.
#[derive(Clone)]
pub struct MissionRuntimePort {
    services: Arc<RuntimeServices>,
}

impl MissionRuntimePort {
    #[must_use]
    pub fn new(services: Arc<RuntimeServices>) -> Self {
        Self { services }
    }

    #[must_use]
    pub fn default_mission_id(&self) -> &str {
        self.mission().default_mission_id()
    }

    pub fn ensure_default_mission(&self) -> Result<(), String> {
        self.mission().ensure_default_mission().map(|_| ())
    }

    #[must_use]
    pub fn projection(&self) -> MissionProjection {
        self.mission().projection(
            self.relations(),
            self.services.agent_runtime(),
            self.services.team_runtime(),
            self.services.approval_queue(),
            self.services.conflict_resolver(),
            self.services.mission_evidence(),
            self.services.mission_schedules().projection(),
        )
    }

    #[must_use]
    pub fn control_projection(
        &self,
        sessions: Vec<MissionControlSessionNode>,
        active_session_id: Option<String>,
    ) -> Result<MissionControlProjection, String> {
        self.mission().ensure_default_mission()?;
        Ok(MissionControlRuntime::projection(
            &self.services,
            sessions,
            active_session_id,
        ))
    }

    pub fn reserve_command(
        &self,
        command: MissionCommand,
    ) -> Result<MissionCommandSagaRecord, String> {
        crate::mission_command_router::reserve_mission_command(&self.services, command)
    }

    pub async fn execute_reserved_runtime_effect(
        &self,
        command_id: &str,
    ) -> Result<MissionCommandSagaRecord, String> {
        crate::mission_command_router::execute_reserved_runtime_effect(&self.services, command_id)
            .await
    }

    pub fn commit_command_effect(
        &self,
        command_id: &str,
        result: Value,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<MissionCommandSagaRecord, String> {
        crate::mission_command_router::commit_mission_effect(
            &self.services,
            command_id,
            result,
            evidence_refs,
        )
    }

    pub fn commit_command_receipt(
        &self,
        command_id: &str,
    ) -> Result<MissionCommandSagaRecord, String> {
        crate::mission_command_router::commit_mission_receipt(&self.services, command_id)
    }

    pub fn finalize_command(&self, command_id: &str) -> Result<MissionCommandSagaRecord, String> {
        crate::mission_command_router::finalize_mission_command(&self.services, command_id)
    }

    pub fn reject_command(
        &self,
        command_id: &str,
        reason: impl Into<String>,
    ) -> Result<MissionCommandSagaRecord, String> {
        crate::mission_command_router::reject_mission_command(&self.services, command_id, reason)
    }

    pub fn command_saga(
        &self,
        command_id: &str,
    ) -> Result<Option<MissionCommandSagaRecord>, String> {
        crate::mission_command_router::mission_command_saga(&self.services, command_id)
    }

    /// Submit an already interpreted command through the graph host. The
    /// interpreter stays side-effect free; this is the only port method that
    /// turns its typed graph into execution.
    pub async fn submit_interpretation(
        &self,
        interpretation: MissionCommandInterpretation,
    ) -> Value {
        let prepared = MissionCommandInterpreter::prepare_submission(interpretation.clone());
        match interpretation.command {
            crate::MissionInterpretedCommand::SubmitExecutionGraph {
                graph,
                graph_command,
            } => {
                let submitted = self
                    .services
                    .execution_supervisor()
                    .submit_graph(graph, graph_command)
                    .await
                    .map(|receipt| serde_json::to_value(receipt).unwrap_or_default())
                    .map_err(|error| error.to_string());
                json!({
                    "ok": submitted.is_ok(),
                    "kind": "runtime.mission_command_submission",
                    "prepared": prepared,
                    "result": submitted,
                })
            }
            crate::MissionInterpretedCommand::Blocked { reason } => json!({
                "ok": false,
                "kind": "runtime.mission_command_submission",
                "prepared": prepared,
                "error": reason,
            }),
        }
    }

    pub fn team_projection(&self, team_id: &str) -> Result<TeamProjection, String> {
        self.services
            .team_runtime()
            .list()?
            .into_iter()
            .find(|team| team.team_id == team_id)
            .ok_or_else(|| format!("team not found: {team_id}"))
    }

    pub fn team_graph(&self, team_id: &str) -> Result<ExecutionGraph, String> {
        let team = self.team_projection(team_id)?;
        self.services
            .graph_state_store()
            .load(&team.graph_id)
            .map_err(|error| error.to_string())
    }

    #[must_use]
    pub fn team_projection_json(&self) -> Value {
        self.services.team_runtime().projection_json()
    }

    pub async fn cancel_team(&self, team_id: &str) -> Result<Value, String> {
        let team = self.team_projection(team_id)?;
        let projection = self
            .services
            .execution_supervisor()
            .graph_projection(&team.graph_id)
            .await
            .map_err(|error| error.to_string())?;
        let receipt = self
            .services
            .execution_supervisor()
            .command_graph(
                &projection.graph_id,
                ExecutionGraphCommand::Cancel {
                    expected_revision: projection.revision,
                    reason: "team cancellation requested through a surface command".to_string(),
                },
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(serde_json::to_value(receipt).unwrap_or_default())
    }

    #[must_use]
    pub fn agent_events(&self, agent_id: &str) -> Value {
        json!({
            "events": self.services.agent_runtime().events(agent_id),
            "run": self.services.agent_runtime().get(agent_id),
        })
    }

    #[must_use]
    pub fn team_evidence(&self, team_id: &str) -> Value {
        let team = self.team_projection(team_id).ok();
        let events = team
            .as_ref()
            .map(|team| {
                team.tasks
                    .iter()
                    .flat_map(|task| self.services.agent_runtime().events(&task.agent_id))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let tasks = team
            .as_ref()
            .map(|team| team.tasks.clone())
            .unwrap_or_default();
        json!({
            "events": events,
            "tasks": tasks,
            "team": team,
            "evidence": self.services.mission_evidence().list_for_team(team_id),
        })
    }

    #[must_use]
    pub fn approvals_projection(&self) -> Value {
        self.services.approval_queue().projection()
    }

    #[must_use]
    pub fn relations_projection(&self) -> Value {
        self.relations().projection()
    }

    #[must_use]
    pub fn conflicts_projection(&self) -> Value {
        self.services.conflict_resolver().projection()
    }

    #[must_use]
    pub fn mission_id_for_session(&self, session_id: &str) -> Option<String> {
        self.mission().mission_id_for_session(session_id)
    }

    pub fn ensure_session_membership(&self, session_id: &str) -> Result<Value, String> {
        let mission = self.mission().ensure_default_mission()?;
        let receipt =
            self.mission()
                .ensure_session_linked(&mission.mission_id, session_id, Vec::new())?;
        Ok(serde_json::to_value(receipt).unwrap_or_default())
    }

    pub fn remove_session_membership(&self, session_id: &str) -> Result<Value, String> {
        let Some(mission_id) = self.mission().mission_id_for_session(session_id) else {
            return Ok(json!({"removed": false, "reason": "not_linked"}));
        };
        let Some(mission) = self.mission().aggregate(&mission_id) else {
            return Ok(json!({"removed": false, "reason": "mission_missing"}));
        };
        let receipt =
            self.mission()
                .unlink_session(&mission_id, mission.revision, session_id, Vec::new())?;
        Ok(serde_json::to_value(receipt).unwrap_or_default())
    }

    #[must_use]
    pub fn schedule_projection(&self) -> Value {
        self.services.mission_schedules().projection()
    }

    #[must_use]
    pub fn schedule_policy(&self) -> MissionSchedulePolicy {
        self.services.mission_schedule_policy()
    }

    pub fn create_schedule(&self, request: CreateMissionScheduleRequest) -> Result<Value, String> {
        self.services
            .mission_schedules()
            .create(request, now_ms())
            .map(|schedule| serde_json::to_value(schedule).unwrap_or_default())
    }

    pub async fn dispatch_due_schedules(&self) -> Result<MissionScheduleDispatchReport, String> {
        self.services.dispatch_due_mission_schedules(now_ms()).await
    }

    pub fn pause_schedule(&self, schedule_id: &str) -> Result<Value, String> {
        self.services
            .mission_schedules()
            .pause(schedule_id)
            .map(|schedule| serde_json::to_value(schedule).unwrap_or_default())
    }

    pub fn resume_schedule(&self, schedule_id: &str) -> Result<Value, String> {
        self.services
            .mission_schedules()
            .resume(schedule_id, now_ms())
            .map(|schedule| serde_json::to_value(schedule).unwrap_or_default())
    }

    pub fn update_schedule(
        &self,
        schedule_id: &str,
        request: UpdateMissionScheduleRequest,
    ) -> Result<Value, String> {
        self.services
            .mission_schedules()
            .update(schedule_id, request, now_ms())
            .map(|schedule| serde_json::to_value(schedule).unwrap_or_default())
    }

    pub async fn instantiate_team(
        &self,
        request: TeamInstantiationRequest,
    ) -> Result<TeamProjection, String> {
        self.services.team_runtime().instantiate(request).await
    }

    pub fn submit_approval(&self, request: SubmitGlobalApprovalRequest) -> Result<Value, String> {
        self.services
            .approval_queue()
            .submit(request)
            .map(|approval| serde_json::to_value(approval).unwrap_or_default())
    }

    pub fn decide_approval(
        &self,
        principal: &crate::VerifiedPrincipal,
        decision: ApprovalDecisionCommand,
    ) -> Result<Value, String> {
        self.services
            .approval_queue()
            .decide(principal, decision)
            .map(|receipt| serde_json::to_value(receipt).unwrap_or_default())
    }

    pub fn upsert_proxy(&self, proxy: SessionProxy) -> Result<SessionProxy, String> {
        self.relations().upsert_proxy(proxy)
    }

    fn mission(&self) -> &MissionRuntime {
        self.services.mission_runtime()
    }

    fn relations(&self) -> &SessionRelationGraph {
        self.services.session_relations()
    }
}

/// Runtime's sole surface-facing Task boundary.
///
/// The aggregate and evidence outbox are committed atomically by the selected
/// Task backend. This port projects pending evidence into Runtime's event log,
/// repairs Mission membership idempotently, and resolves graph references from
/// the canonical graph store instead of caching graph projections in Task.
#[derive(Clone)]
pub struct TaskRuntimePort {
    tasks: Arc<crate::TaskAggregateService>,
    missions: Arc<MissionRuntime>,
    events: Arc<crate::RuntimeEventStore>,
    graphs: crate::ExecutionGraphStateStore,
    commits: crate::ExecutionCommitService,
}

impl TaskRuntimePort {
    #[must_use]
    pub fn new(services: &RuntimeServices) -> Self {
        Self::from_components(
            Arc::clone(services.task_aggregate_service()),
            Arc::clone(services.mission_runtime()),
            Arc::clone(services.event_store()),
            services.graph_state_store().clone(),
            services.commit_service().clone(),
        )
    }

    pub(crate) fn from_components(
        tasks: Arc<crate::TaskAggregateService>,
        missions: Arc<MissionRuntime>,
        events: Arc<crate::RuntimeEventStore>,
        graphs: crate::ExecutionGraphStateStore,
        commits: crate::ExecutionCommitService,
    ) -> Self {
        Self {
            tasks,
            missions,
            events,
            graphs,
            commits,
        }
    }

    pub fn list(&self) -> Result<Vec<TaskAggregate>, String> {
        self.tasks.list()
    }

    pub fn get(&self, task_id: &str) -> Result<Option<TaskAggregate>, String> {
        self.tasks.get(task_id)
    }

    pub fn current(&self) -> Result<Option<TaskAggregate>, String> {
        self.tasks.current()
    }

    #[must_use]
    pub fn workspace_default_mission_id(&self) -> &str {
        self.missions.default_mission_id()
    }

    #[must_use]
    pub fn mission_id_for_session(&self, session_id: &str) -> String {
        self.missions
            .mission_id_for_session(session_id)
            .unwrap_or_else(|| self.missions.default_mission_id().to_string())
    }

    pub fn latest_terminal_runtime_receipt(
        &self,
        task_id: &str,
    ) -> Result<Option<crate::DurableRuntimeEvent>, String> {
        if task_id.trim().is_empty() {
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
        record_task_terminal_observation(
            &self.events,
            task_id,
            terminal_status,
            source_receipt_ref,
            correlation_id,
        )
    }

    pub fn link_mission_team_run(
        &self,
        mission_id: &str,
        team_run_id: &str,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<(), String> {
        self.missions
            .ensure_team_run_linked(mission_id, team_run_id, evidence_refs)
            .map(|_| ())
    }

    pub fn link_mission_agent_run(
        &self,
        mission_id: &str,
        agent_run_id: &str,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<(), String> {
        self.missions
            .ensure_agent_run_linked(mission_id, agent_run_id, evidence_refs)
            .map(|_| ())
    }

    pub fn create(&self, command: TaskCreateCommand) -> Result<crate::TaskCommandOutcome, String> {
        let mission_id = command.mission_id.clone();
        let session_id = command.source_session_id.clone();
        let evidence_refs = command.evidence_refs.clone();
        self.ensure_mission_active(&mission_id, &evidence_refs)?;
        let result = self.tasks.create(command)?;
        let outcome = self.finish_mutation(result)?;
        self.link_mission_entity(
            &mission_id,
            &session_id,
            &outcome.aggregate.task_id,
            &evidence_refs,
        )?;
        Ok(outcome)
    }

    pub fn start_phase(
        &self,
        task_id: &str,
        expected_revision: u64,
        spec: TaskPhaseSpec,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<crate::TaskCommandOutcome, String> {
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
        let mut projections = Vec::new();
        for task in self.list()? {
            for reference in task.graph_refs {
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

    pub async fn register_execution_graph(
        &self,
        task_id: &str,
        objective: Option<String>,
        nodes: Vec<ExecutionNodeSpec>,
        edges: Vec<ExecutionEdge>,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<ExecutionGraphProjection, String> {
        if nodes.is_empty() {
            return Err("execution graph requires at least one node".to_string());
        }
        let task = self
            .get(task_id)?
            .ok_or_else(|| format!("task `{task_id}` not found"))?;
        let mut graph = ExecutionGraph::new(objective.unwrap_or_else(|| task.objective.clone()));
        graph.id = format!("execution-graph-task-{task_id}");
        graph.service_class = harness_contract::execution_graph::ExecutionServiceClass::Foreground;
        graph.nodes = nodes;
        graph.edges = edges;
        validate_execution_graph(&graph).map_err(|error| error.to_string())?;
        let receipt = self
            .commits
            .register_graph_async(graph)
            .await
            .map_err(|error| error.to_string())?;
        let projection = self
            .graphs
            .projection_async(receipt.graph.id)
            .await
            .map_err(|error| error.to_string())?;
        let result = self.tasks.link_graph(
            task_id,
            task.revision,
            projection.graph_id.clone(),
            projection.revision,
            evidence_refs.clone(),
        )?;
        self.finish_mutation(result)?;
        self.link_mission_graph(&task.mission_id, &projection.graph_id, evidence_refs)?;
        Ok(projection)
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
        let mission_id = task.mission_id.clone();
        let result = self.tasks.link_graph(
            task_id,
            task.revision,
            graph_id,
            graph_revision,
            evidence_refs.clone(),
        )?;
        let outcome = self.finish_mutation(result)?;
        self.link_mission_graph(&mission_id, graph_id, evidence_refs)?;
        Ok(outcome)
    }

    /// Replay all unprojected Task evidence. Startup recovery and post-mutation
    /// callers use the same idempotent path.
    pub fn project_pending_evidence(&self, limit: usize) -> Result<usize, String> {
        let records = self.tasks.pending_outbox(None, limit)?;
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
                            id: task.source_session_id,
                        },
                        RuntimeEventRef {
                            kind: "turn".to_string(),
                            id: task.source_turn_id,
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
    /// the requested data, while startup drains the durable outbox and repairs
    /// Mission references exactly once.
    pub fn recover(&self) -> Result<(), String> {
        const RECOVERY_BATCH_SIZE: usize = 256;
        loop {
            let projected = self.project_pending_evidence(RECOVERY_BATCH_SIZE)?;
            if projected < RECOVERY_BATCH_SIZE {
                break;
            }
        }
        self.repair_membership()
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
        self.project_pending_evidence(256)?;
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

    fn link_mission_entity(
        &self,
        mission_id: &str,
        session_id: &str,
        task_id: &str,
        evidence_refs: &[EvidenceRef],
    ) -> Result<(), String> {
        self.missions
            .ensure_session_linked(mission_id, session_id, evidence_refs.to_vec())?;
        let mut task_evidence = evidence_refs.to_vec();
        task_evidence.push(EvidenceRef::observed("task", task_id).with_source("runtime.task"));
        self.missions
            .ensure_task_linked(mission_id, task_id, task_evidence)
            .map(|_| ())
    }

    fn link_mission_graph(
        &self,
        mission_id: &str,
        graph_id: &str,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<(), String> {
        self.missions
            .ensure_graph_linked(mission_id, graph_id, evidence_refs)
            .map(|_| ())
    }

    fn repair_membership(&self) -> Result<(), String> {
        for task in self.tasks.list()? {
            self.ensure_mission_active(&task.mission_id, &[])?;
            self.link_mission_entity(
                &task.mission_id,
                &task.source_session_id,
                &task.task_id,
                &[],
            )?;
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::task::TaskSpec;
    use std::sync::{Arc, Barrier};

    #[test]
    fn task_survives_restart_and_can_continue_from_another_session() {
        let root = tempfile::tempdir().expect("runtime root");
        let home = root.path().join("home");
        let workspace = root.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let services = RuntimeServices::builder(&home, &workspace)
            .build()
            .expect("runtime services");
        let mission = services
            .mission_runtime()
            .ensure_default_mission()
            .expect("default Mission");
        let mission_id = mission.mission_id.clone();
        services
            .mission_runtime()
            .link_session(&mission_id, mission.revision, "session-a", Vec::new())
            .expect("source Session membership");
        let created = services
            .task_runtime_port()
            .create(TaskCreateCommand {
                task_id: "task-cross-session-restart".to_string(),
                mission_id: mission_id.clone(),
                source_session_id: "session-a".to_string(),
                source_turn_id: "turn-a".to_string(),
                spec: TaskSpec::new("continue after process restart"),
                evidence_refs: vec![EvidenceRef::observed(
                    "test_fixture",
                    "test://task/cross-session-restart",
                )],
            })
            .expect("create canonical Task");
        assert!(services
            .mission_runtime()
            .aggregate(&mission_id)
            .expect("default Mission")
            .task_refs
            .iter()
            .any(|reference| reference.id == created.task_id));
        drop(services);

        let restarted = RuntimeServices::builder(&home, &workspace)
            .build()
            .expect("restarted Runtime");
        let mission = restarted
            .mission_runtime()
            .aggregate(&mission_id)
            .expect("restarted Mission");
        restarted
            .mission_runtime()
            .link_session(&mission_id, mission.revision, "session-b", Vec::new())
            .expect("continuation Session membership");
        let loaded = restarted
            .task_runtime_port()
            .get("task-cross-session-restart")
            .expect("Task lookup")
            .expect("Task survives");
        let continued = restarted
            .task_runtime_port()
            .record_failure(
                &loaded.task_id,
                loaded.revision,
                "session B records a recoverable observation",
                Vec::new(),
            )
            .expect("Session B continues Task");
        assert_eq!(continued.source_session_id, "session-a");
        assert_eq!(continued.source_turn_id, "turn-a");
        assert_eq!(continued.failure_count, 1);
        assert!(restarted
            .mission_runtime()
            .aggregate(&mission_id)
            .expect("recovered Mission")
            .session_refs
            .iter()
            .any(|reference| reference.id == "session-a"));
    }

    #[test]
    fn concurrent_task_creates_preserve_every_mission_membership() {
        const WRITERS: usize = 32;
        let root = tempfile::tempdir().expect("runtime root");
        let workspace = root.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let services = RuntimeServices::builder(root.path().join("home"), &workspace)
            .build()
            .expect("runtime services");
        let mission_id = services.mission_runtime().default_mission_id().to_string();
        let task_port = services.task_runtime_port();
        let barrier = Arc::new(Barrier::new(WRITERS));
        let writers = (0..WRITERS)
            .map(|index| {
                let task_port = task_port.clone();
                let barrier = Arc::clone(&barrier);
                let mission_id = mission_id.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    task_port.create(TaskCreateCommand {
                        task_id: format!("concurrent-task-{index}"),
                        mission_id,
                        source_session_id: format!("concurrent-session-{index}"),
                        source_turn_id: format!("concurrent-turn-{index}"),
                        spec: TaskSpec::new(format!("concurrent objective {index}")),
                        evidence_refs: Vec::new(),
                    })
                })
            })
            .collect::<Vec<_>>();
        for writer in writers {
            writer
                .join()
                .expect("Task writer thread")
                .expect("concurrent Task create");
        }
        let mission = services
            .mission_runtime()
            .aggregate(&mission_id)
            .expect("default Mission");
        assert_eq!(mission.task_refs.len(), WRITERS);
        assert_eq!(mission.session_refs.len(), WRITERS);
        for index in 0..WRITERS {
            assert!(mission
                .task_refs
                .iter()
                .any(|reference| reference.id == format!("concurrent-task-{index}")));
            assert!(mission
                .session_refs
                .iter()
                .any(|reference| reference.id == format!("concurrent-session-{index}")));
        }
    }
}
