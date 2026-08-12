//! Narrow Runtime-owned Mission query and command port.
//!
//! Gateway, TUI, and other surfaces must not assemble Mission projections or
//! mutate Mission-adjacent aggregates through `RuntimeServices` directly.
//! This port keeps those aggregate joins inside Runtime while preserving the
//! existing event-sourced stores as the single authority.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphCommand, ExecutionGraphLineage,
};
use harness_contract::mission::{
    MissionCommand, MissionCommandSagaRecord, MissionControlProjection, MissionControlSessionNode,
};
use harness_contract::reality::EvidenceRef;
use harness_contract::task::{TaskOrigin, TaskRouteHint};
use harness_contract::team::TeamInstantiationRequest;
use serde_json::{json, Value};

use crate::{
    ApprovalDecisionCommand, CreateMissionScheduleRequest, ExecutionGraphHost,
    MissionCommandInterpretation, MissionCommandInterpreter, MissionControlRuntime,
    MissionProjection, MissionRuntime, MissionScheduleDispatchReport, MissionSchedulePolicy,
    RuntimeServices, SessionProxy, SessionRelationGraph, SubmitGlobalApprovalRequest,
    TeamProjection, UpdateMissionScheduleRequest,
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
    pub fn has_default_mission(&self) -> bool {
        self.mission()
            .aggregate(self.mission().default_mission_id())
            .is_some()
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
        selected_mission_id: Option<String>,
    ) -> Result<MissionControlProjection, String> {
        self.mission().ensure_default_mission()?;
        Ok(MissionControlRuntime::projection(
            &self.services,
            sessions,
            active_session_id,
            selected_mission_id,
        ))
    }

    /// Return sessions that contribute to at least one Task.
    #[must_use]
    pub fn referenced_session_ids(&self) -> Vec<String> {
        let tasks = self.services.task_aggregate_service();
        let mut session_ids = tasks
            .list()
            .unwrap_or_default()
            .into_iter()
            .flat_map(|task| {
                let mut sessions = vec![task.origin_session_id.clone()];
                sessions.extend(
                    tasks
                        .bindings_for_task(&task.task_id)
                        .unwrap_or_default()
                        .into_iter()
                        .map(|binding| binding.session_id),
                );
                sessions
            })
            .collect::<Vec<_>>();
        session_ids.sort();
        session_ids.dedup();
        session_ids
    }

    /// Derive Session participation from canonical Task/Turn bindings.
    /// Mission never stores a second Session membership list.
    #[must_use]
    pub fn session_task_contributions(&self) -> BTreeMap<String, Vec<String>> {
        let tasks = self.services.task_aggregate_service();
        let mut contributions = BTreeMap::<String, BTreeSet<String>>::new();
        for task in tasks.list().unwrap_or_default() {
            contributions
                .entry(task.origin_session_id.clone())
                .or_default()
                .insert(task.task_id.clone());
            for binding in tasks.bindings_for_task(&task.task_id).unwrap_or_default() {
                contributions
                    .entry(binding.session_id)
                    .or_default()
                    .insert(task.task_id.clone());
            }
        }
        contributions
            .into_iter()
            .map(|(session_id, task_ids)| (session_id, task_ids.into_iter().collect()))
            .collect()
    }

    #[must_use]
    pub fn mission_ids_for_session(&self, session_id: &str) -> Vec<String> {
        let tasks = self.services.task_aggregate_service();
        let mut mission_ids = BTreeSet::new();
        for task in tasks.list().unwrap_or_default() {
            let contributes = task.origin_session_id == session_id
                || tasks
                    .bindings_for_task(&task.task_id)
                    .unwrap_or_default()
                    .iter()
                    .any(|binding| binding.session_id == session_id);
            if contributes {
                mission_ids.insert(task.mission_id);
            }
        }
        mission_ids.into_iter().collect()
    }

    #[must_use]
    pub fn session_ids_for_missions(&self, mission_ids: &[String]) -> Vec<String> {
        let mission_ids = mission_ids
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let tasks = self.services.task_aggregate_service();
        let mut session_ids = BTreeSet::new();
        for task in tasks.list().unwrap_or_default() {
            if !mission_ids.contains(task.mission_id.as_str()) {
                continue;
            }
            session_ids.insert(task.origin_session_id.clone());
            session_ids.extend(
                tasks
                    .bindings_for_task(&task.task_id)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|binding| binding.session_id),
            );
        }
        session_ids.into_iter().collect()
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
                let graph_id = graph.id.clone();
                let task_id = graph
                    .lineage
                    .as_ref()
                    .map(|lineage| lineage.task_id.clone());
                let submitted = self
                    .services
                    .execution_supervisor()
                    .submit_graph(graph, graph_command)
                    .await
                    .map_err(|error| error.to_string())
                    .and_then(|receipt| {
                        if let Some(task_id) = task_id.as_deref() {
                            self.services.task_runtime_port().link_existing_graph(
                                task_id,
                                &graph_id,
                                receipt.accepted_revision,
                                vec![EvidenceRef::observed("execution_graph", graph_id.clone())],
                            )?;
                        }
                        Ok(serde_json::to_value(receipt).unwrap_or_default())
                    })
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

    /// Materialize the Root Task/Turn binding that owns one Mission command,
    /// then attach its canonical lineage to the executable graph. Preview-only
    /// interpretations remain side-effect free and never call this method.
    pub async fn bind_task_lineage(
        &self,
        interpretation: MissionCommandInterpretation,
        source_session_id: &str,
        session_generation: u64,
        origin: TaskOrigin,
        route_hint: Option<TaskRouteHint>,
    ) -> Result<MissionCommandInterpretation, String> {
        let graph_id = match &interpretation.command {
            crate::MissionInterpretedCommand::SubmitExecutionGraph { graph, .. } => {
                graph.id.clone()
            }
            crate::MissionInterpretedCommand::Blocked { .. } => return Ok(interpretation),
        };
        let request_id = format!("mission-command:{graph_id}");
        let turn_id = format!("turn:{request_id}");
        let input_id = format!("input:{request_id}");
        let handoff_id = route_hint.as_ref().and_then(|hint| hint.handoff_id.clone());
        let route = crate::materialize_session_task_route(
            &self.services,
            &crate::TaskRouter,
            &request_id,
            &input_id,
            source_session_id,
            &turn_id,
            &interpretation.command_text,
            self.default_mission_id(),
            route_hint,
            origin,
            None,
        )
        .await?;
        let mut target_hint = TaskRouteHint {
            task_id: Some(route.root_task.task_id.clone()),
            mission_id: Some(route.root_task.mission_id.clone()),
            ..TaskRouteHint::default()
        };
        if let Some(handoff_id) = handoff_id {
            target_hint.handoff_id = Some(handoff_id);
        }
        MissionCommandInterpreter::bind_execution_lineage(
            interpretation,
            ExecutionGraphLineage {
                session_id: source_session_id.to_string(),
                turn_id,
                root_task_id: route.root_task.task_id,
                task_id: route.primary_task.task_id,
                generation: session_generation.max(1),
            },
            Some(target_hint),
        )
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

    pub async fn run_schedule_now(
        &self,
        schedule_id: &str,
    ) -> Result<MissionScheduleDispatchReport, String> {
        if !self.services.mission_schedule_policy().enabled {
            return Err("mission scheduling is disabled by runtime policy".to_string());
        }
        self.services
            .mission_schedules()
            .trigger_now(schedule_id, now_ms())?;
        self.services.dispatch_due_mission_schedules(now_ms()).await
    }

    pub fn delete_schedule(&self, schedule_id: &str) -> Result<Value, String> {
        self.services
            .mission_schedules()
            .delete(schedule_id)
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

    pub fn submit_approval(
        &self,
        mut request: SubmitGlobalApprovalRequest,
    ) -> Result<Value, String> {
        request.context = self
            .services
            .bind_session_policy_to_approval_context(request.context);
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
        let approval_id = decision.approval_id.clone();
        let receipt = self
            .services
            .approval_queue()
            .decide(principal, decision)
            .map(|receipt| serde_json::to_value(receipt).unwrap_or_default())?;
        self.services
            .approval_coordinator()
            .notify_decision(&approval_id);
        Ok(receipt)
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

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
