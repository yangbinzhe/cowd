//! Narrow Runtime-owned Mission query and command port.
//!
//! Gateway, TUI, and other surfaces must not assemble Mission projections or
//! mutate Mission-adjacent aggregates through `RuntimeServices` directly.
//! This port keeps those aggregate joins inside Runtime while preserving the
//! existing event-sourced stores as the single authority.

use std::sync::Arc;

use harness_contract::execution_graph::{ExecutionGraph, ExecutionGraphCommand};
use serde_json::{json, Value};

use crate::{
    CreateMissionScheduleRequest, ExecutionGraphHost, GlobalApprovalDecision,
    MissionCommandInterpretation, MissionCommandInterpreter, MissionControlCommand,
    MissionControlCommandReceipt, MissionControlProjection, MissionControlRuntime,
    MissionProjection, MissionRuntime, MissionScheduleDispatchReport, MissionSchedulePolicy,
    MissionSessionSnapshot, RuntimeServices, SessionProxy, SessionRelation, SessionRelationGraph,
    SessionRelationKind, StartMissionSessionRequest, StartTeamRequest, SubmitGlobalApprovalRequest,
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
    pub fn control_projection(&self) -> MissionControlProjection {
        MissionControlRuntime::projection(&self.services)
    }

    pub async fn execute_control(
        &self,
        command: MissionControlCommand,
    ) -> MissionControlCommandReceipt {
        MissionControlRuntime::execute(command, &self.services).await
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
                    .graph_runner()
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
            .graph_runner()
            .graph_projection(&team.graph_id)
            .await
            .map_err(|error| error.to_string())?;
        let receipt = self
            .services
            .graph_runner()
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

    pub fn start_session(
        &self,
        request: StartMissionSessionRequest,
    ) -> Result<MissionSessionSnapshot, String> {
        self.mission().start_session(request)
    }

    pub fn session(&self, session_id: &str) -> Option<MissionSessionSnapshot> {
        self.mission().get_session(session_id)
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
        if self
            .mission()
            .get_session(&request.target_session_id)
            .is_none()
        {
            return Err(format!(
                "mission target session not found: {}",
                request.target_session_id
            ));
        }
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

    pub async fn start_team(&self, request: StartTeamRequest) -> Result<TeamProjection, String> {
        self.services.team_runtime().start(request).await
    }

    pub fn submit_approval(&self, request: SubmitGlobalApprovalRequest) -> Result<Value, String> {
        self.services
            .approval_queue()
            .submit(request)
            .map(|approval| serde_json::to_value(approval).unwrap_or_default())
    }

    pub fn decide_approval(&self, decision: GlobalApprovalDecision) -> Result<Value, String> {
        self.services
            .approval_queue()
            .decide(decision)
            .map(|receipt| serde_json::to_value(receipt).unwrap_or_default())
    }

    pub fn add_relation(
        &self,
        from_session_id: String,
        to_session_id: String,
        kind: SessionRelationKind,
        summary: String,
        evidence_refs: Vec<String>,
    ) -> Result<SessionRelation, String> {
        self.relations()
            .add_relation(from_session_id, to_session_id, kind, summary, evidence_refs)
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
