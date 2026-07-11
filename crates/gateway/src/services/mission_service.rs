use serde::{Deserialize, Serialize};

use super::{service_envelope, MissionService, ServiceEnvelope};

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct StartMissionSessionHttpRequest {
    pub(crate) title: String,
    #[serde(default)]
    pub(crate) session_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct AttachMissionTeamHttpRequest {
    pub(crate) team_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct AttachMissionAgentHttpRequest {
    pub(crate) agent_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StartMissionTeamRuntimeHttpRequest {
    pub(crate) objective: String,
    #[serde(default)]
    pub(crate) model: Option<String>,
    #[serde(default)]
    pub(crate) execution_mode: MissionTeamExecutionMode,
    #[serde(default)]
    pub(crate) approval_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct MissionTeamHandoffHttpRequest {
    #[serde(default)]
    pub(crate) target: Option<String>,
    #[serde(default)]
    pub(crate) note: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SubmitAgentTaskOutcomeHttpRequest {
    pub(crate) result_summary: String,
    #[serde(default)]
    pub(crate) evidence_refs: Vec<String>,
    #[serde(default)]
    pub(crate) conflicts: Vec<String>,
    #[serde(default)]
    pub(crate) suggested_next_actions: Vec<String>,
    #[serde(default)]
    pub(crate) quality_status: runtime::AgentTaskQualityStatus,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MissionTeamExecutionMode {
    #[default]
    ProviderInProcess,
    ProcessJsonl,
    ManualMailbox,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SubmitMissionApprovalHttpRequest {
    pub(crate) source: runtime::ApprovalSource,
    pub(crate) action: String,
    pub(crate) summary: String,
    pub(crate) risk: harness_contract::core::TaskRisk,
    #[serde(default)]
    pub(crate) evidence_refs: Vec<String>,
    pub(crate) timeout_policy: runtime::ApprovalTimeoutPolicy,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct DecideMissionApprovalHttpRequest {
    pub(crate) approved: bool,
    pub(crate) decided_by: String,
    #[serde(default)]
    pub(crate) reason: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct AddMissionRelationHttpRequest {
    pub(crate) from_session_id: String,
    pub(crate) to_session_id: String,
    pub(crate) kind: runtime::SessionRelationKind,
    pub(crate) summary: String,
    #[serde(default)]
    pub(crate) evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct UpsertMissionProxyHttpRequest {
    pub(crate) session_id: String,
    pub(crate) summary: String,
    #[serde(default)]
    pub(crate) evidence_refs: Vec<String>,
    #[serde(default)]
    pub(crate) decisions: Vec<String>,
    #[serde(default)]
    pub(crate) open_questions: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RouteMissionCommandHttpRequest {
    pub(crate) from_session_id: String,
    pub(crate) target_ref: String,
    pub(crate) command: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct InterpretMissionCommandHttpRequest {
    pub(crate) current_session_id: String,
    pub(crate) command_text: String,
    #[serde(default)]
    pub(crate) target_ref: Option<String>,
    #[serde(default)]
    pub(crate) autonomy_policy: Option<runtime::StewardAutomationPolicy>,
    #[serde(default)]
    pub(crate) dispatch_mode: Option<runtime::SessionDispatchMode>,
    #[serde(default)]
    pub(crate) allow_background: Option<bool>,
    #[serde(default)]
    pub(crate) execute: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ConsumeMissionSessionCommandHttpRequest {
    #[serde(default)]
    pub(crate) actor_id: Option<String>,
    #[serde(default)]
    pub(crate) mode: MissionSessionCommandConsumeMode,
    #[serde(default)]
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MissionSessionCommandConsumeMode {
    #[default]
    MarkClaimedOnly,
    StartTurn,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct StartMissionStewardHttpRequest {
    pub(crate) mission_id: String,
    #[serde(default)]
    pub(crate) root_session_id: Option<String>,
    pub(crate) profile_id: runtime::AutonomyProfileId,
    pub(crate) objective: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct TickMissionStewardHttpRequest {
    #[serde(default)]
    pub(crate) action: Option<String>,
    #[serde(default)]
    pub(crate) summary: Option<String>,
    #[serde(default)]
    pub(crate) risk: Option<harness_contract::core::TaskRisk>,
    #[serde(default)]
    pub(crate) requested_tool: Option<String>,
    #[serde(default)]
    pub(crate) requires_write: bool,
    #[serde(default)]
    pub(crate) is_critical_operation: bool,
    #[serde(default)]
    pub(crate) evidence_refs: Vec<String>,
    #[serde(default)]
    pub(crate) timeout_policy: Option<runtime::ApprovalTimeoutPolicy>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct InterruptMissionStewardHttpRequest {
    #[serde(default)]
    pub(crate) reason: Option<String>,
}

impl MissionService {
    pub(crate) fn new() -> Self {
        Self {
            label: "mission",
            owner: "0.9.380 Mission Runtime service boundary",
            runtime_services: None,
        }
    }

    pub(crate) fn with_runtime_services(
        mut self,
        runtime_services: std::sync::Arc<runtime::RuntimeServices>,
    ) -> Self {
        self.runtime_services = Some(runtime_services);
        self
    }

    fn runtime_services(&self) -> &runtime::RuntimeServices {
        self.runtime_services
            .as_deref()
            .expect("MissionService requires RuntimeServices")
    }

    fn mission(&self) -> &runtime::MissionRuntime {
        self.runtime_services().mission_runtime()
    }

    fn relation_graph(&self) -> &runtime::SessionRelationGraph {
        self.runtime_services().session_relations()
    }

    fn mission_projection(&self) -> runtime::MissionProjection {
        self.mission().projection(self.relation_graph())
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }

    pub(crate) fn projection_contract(&self) -> ServiceEnvelope {
        self.envelope("projection")
    }

    pub(crate) fn session_control_contract(&self) -> ServiceEnvelope {
        self.envelope("session_control")
    }

    pub(crate) fn approval_projection_contract(&self) -> ServiceEnvelope {
        self.envelope("approval_projection")
    }

    pub(crate) fn relation_projection_contract(&self) -> ServiceEnvelope {
        self.envelope("relation_projection")
    }

    pub(crate) fn conflict_projection_contract(&self) -> ServiceEnvelope {
        self.envelope("conflict_projection")
    }

    pub(crate) fn approval_command_contract(&self) -> ServiceEnvelope {
        self.envelope("approval_command")
    }

    pub(crate) fn relation_command_contract(&self) -> ServiceEnvelope {
        self.envelope("relation_command")
    }

    pub(super) fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![
            self.projection_contract(),
            self.session_control_contract(),
            self.approval_projection_contract(),
            self.relation_projection_contract(),
            self.conflict_projection_contract(),
            self.approval_command_contract(),
            self.relation_command_contract(),
        ]
    }

    pub(crate) fn projection(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.projection_contract(),
            "mission": self.mission_projection(),
            "stewards": runtime::global_steward_runtime_service().projection(),
        })
    }

    pub(crate) fn mission_control(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "projection": runtime::MissionControlRuntime::projection(self.runtime_services()),
        })
    }

    pub(crate) fn execute_mission_control_command(
        &self,
        command: runtime::MissionControlCommand,
    ) -> serde_json::Value {
        let receipt = runtime::MissionControlRuntime::execute(command, self.runtime_services());
        let ok = !matches!(
            receipt.status,
            runtime::MissionControlCommandStatus::Failed
                | runtime::MissionControlCommandStatus::Rejected
        );
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.command_result",
            "ok": ok,
            "receipt": receipt,
            "projection": runtime::MissionControlRuntime::projection(self.runtime_services()),
        })
    }

    pub(crate) fn dispatch_mission_sessions(
        &self,
        policy: runtime::SessionExecutionPolicy,
    ) -> serde_json::Value {
        let interpretation = runtime::MissionCommandInterpreter::interpret_session_policy(policy);
        let receipt = runtime::MissionCommandInterpreter::prepare_submission(interpretation);
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.session_dispatch_submission",
            "ok": receipt.ok,
            "receipt": receipt,
            "projection": runtime::MissionControlRuntime::projection(self.runtime_services()),
        })
    }

    pub(crate) fn bridge_mission_session(
        &self,
        message: runtime::CrossSessionMessage,
    ) -> serde_json::Value {
        let receipt = runtime::MissionCommandInterpreter::prepare_submission(
            runtime::MissionCommandInterpreter::interpret_session_message(message),
        );
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.session_bridge_submission",
            "ok": receipt.ok,
            "receipt": receipt,
            "projection": runtime::MissionControlRuntime::projection(self.runtime_services()),
        })
    }

    pub(crate) fn interpret_mission_command(
        &self,
        request: InterpretMissionCommandHttpRequest,
    ) -> serde_json::Value {
        let interpretation = runtime::MissionCommandInterpreter::interpret(
            runtime::MissionCommandInterpretRequest {
                current_session_id: request.current_session_id,
                command_text: request.command_text,
                target_ref: request.target_ref,
                autonomy_policy: request.autonomy_policy,
                dispatch_mode: request.dispatch_mode,
                allow_background: request.allow_background,
            },
        );
        let execution = request.execute.then(|| {
            runtime::MissionCommandInterpreter::prepare_submission(interpretation.clone())
        });
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.command_interpretation",
            "ok": interpretation.status == "interpreted"
                && execution.as_ref().map(|receipt| receipt.ok).unwrap_or(true),
            "interpretation": interpretation,
            "execution": execution,
            "projection": runtime::MissionControlRuntime::projection(self.runtime_services()),
        })
    }

    pub(crate) fn team_execution_plan(&self, team_id: &str) -> Result<serde_json::Value, String> {
        let plan = runtime::TeamExecutionLoop::plan(team_id)?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.team_execution_plan",
            "ok": true,
            "plan": plan,
        }))
    }

    pub(crate) fn tick_team_execution(&self, team_id: &str) -> Result<serde_json::Value, String> {
        let plan = runtime::TeamExecutionLoop::plan(team_id)?;
        let command = harness_contract::execution_graph::ExecutionGraphCommand::Start {
            expected_revision: plan.execution_graph.revision,
        };
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.team_execution_graph_submission",
            "ok": false,
            "status": "capability_unavailable",
            "capability": "collaborate",
            "available_in": "V5",
            "graph": plan.execution_graph,
            "command": command,
            "projection": runtime::MissionControlRuntime::projection(self.runtime_services()),
        }))
    }

    pub(crate) fn submit_agent_task_outcome(
        &self,
        team_id: &str,
        task_id: &str,
        request: SubmitAgentTaskOutcomeHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let task = runtime::global_agent_task_mailbox()
            .get(task_id)
            .ok_or_else(|| format!("agent task not found: {task_id}"))?;
        if task.team_id != team_id {
            return Err(format!(
                "agent task {task_id} belongs to team {}, not {team_id}",
                task.team_id
            ));
        }
        let plan = runtime::TeamExecutionLoop::plan(team_id)?;
        let command = harness_contract::execution_graph::ExecutionGraphCommand::Advance {
            expected_revision: plan.execution_graph.revision,
        };
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.agent_task_outcome_submission",
            "ok": false,
            "status": "runtime_host_required",
            "message": "agent outcomes must be committed by ExecutionGraphRunner",
            "task_id": task_id,
            "outcome": {
                "result_summary": request.result_summary,
                "evidence_refs": request.evidence_refs,
                "conflicts": request.conflicts,
                "suggested_next_actions": request.suggested_next_actions,
                "quality_status": request.quality_status,
            },
            "graph": plan.execution_graph,
            "command": command,
            "run": runtime::global_team_runtime_service().collaboration_run(team_id).ok(),
            "projection": runtime::MissionControlRuntime::projection(self.runtime_services()),
        }))
    }

    pub(crate) fn collaboration_runs(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.collaboration_runs",
            "ok": true,
            "projection": runtime::global_team_runtime_service().collaboration_projection(),
        })
    }

    pub(crate) fn collaboration_run(&self, team_id: &str) -> Result<serde_json::Value, String> {
        let run = runtime::global_team_runtime_service().collaboration_run(team_id)?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.collaboration_run",
            "ok": true,
            "run": run,
        }))
    }

    pub(crate) async fn cancel_team_runtime(
        &self,
        team_id: &str,
    ) -> Result<serde_json::Value, String> {
        use runtime::ExecutionGraphHost;

        let plan = runtime::TeamExecutionLoop::plan(team_id)?;
        let projection = match self
            .runtime_services()
            .graph_runner()
            .graph_projection(&plan.execution_graph.id)
            .await
        {
            Ok(projection) => projection,
            Err(_) => {
                return Ok(team_capability_unavailable(
                    self,
                    "mission_control.team_cancel",
                    team_id,
                    "collaborate",
                    "V5",
                ))
            }
        };
        let receipt = self
            .runtime_services()
            .graph_runner()
            .command_graph(
                &projection.graph_id,
                harness_contract::execution_graph::ExecutionGraphCommand::Cancel {
                    expected_revision: projection.revision,
                    reason: "team cancellation requested through Gateway".to_string(),
                },
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.team_cancel",
            "ok": true,
            "receipt": receipt,
        }))
    }

    pub(crate) fn handoff_team_runtime(
        &self,
        team_id: &str,
        request: MissionTeamHandoffHttpRequest,
    ) -> Result<serde_json::Value, String> {
        runtime::TeamExecutionLoop::plan(team_id)?;
        let mut response = team_capability_unavailable(
            self,
            "mission_control.team_handoff",
            team_id,
            "collaborate",
            "V5",
        );
        response["requested_target"] = serde_json::json!(request.target);
        response["requested_note"] = serde_json::json!(request.note);
        Ok(response)
    }

    pub(crate) fn synthesize_team_runtime(
        &self,
        team_id: &str,
    ) -> Result<serde_json::Value, String> {
        runtime::TeamExecutionLoop::plan(team_id)?;
        Ok(team_capability_unavailable(
            self,
            "mission_control.team_synthesis",
            team_id,
            "collaborate",
            "V5",
        ))
    }

    pub(crate) fn agent_mission_events(&self, agent_id: &str) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.agent_events",
            "ok": true,
            "agent_id": agent_id,
            "events": runtime::global_agent_event_bus().list_for_agent(agent_id),
            "tasks": runtime::global_agent_task_mailbox().list_for_agent(agent_id),
        })
    }

    pub(crate) fn team_mission_evidence(&self, team_id: &str) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.team_evidence",
            "ok": true,
            "team_id": team_id,
            "events": runtime::global_agent_event_bus().list_for_team(team_id),
            "tasks": runtime::global_agent_task_mailbox().list_for_team(team_id),
            "evidence": self.runtime_services().mission_evidence().list_for_team(team_id),
        })
    }

    pub(crate) fn approvals(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.approval_projection_contract(),
            "approvals": self.runtime_services().approval_queue().projection(),
        })
    }

    pub(crate) fn relations(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.relation_projection_contract(),
            "relations": self.relation_graph().projection(),
        })
    }

    pub(crate) fn conflicts(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.conflict_projection_contract(),
            "conflicts": self.runtime_services().conflict_resolver().projection(),
        })
    }

    pub(crate) fn stewards(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "stewards": runtime::global_steward_runtime_service().projection(),
        })
    }

    pub(crate) fn tick_all_stewards(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": false,
            "status": "capability_unavailable",
            "capability": "steward_execution",
            "available_in": "V8",
            "side_effects_started": false,
            "stewards": runtime::global_steward_runtime_service().projection(),
        })
    }

    pub(crate) fn steward_scheduler(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.steward_scheduler",
            "ok": true,
            "scheduler": runtime::StewardScheduler::projection(),
        })
    }

    pub(crate) fn tick_steward_scheduler(
        &self,
        config: runtime::StewardSchedulerConfig,
    ) -> serde_json::Value {
        let report = runtime::StewardScheduler::tick(config, self.runtime_services());
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.steward_scheduler_tick",
            "ok": false,
            "status": "capability_unavailable",
            "capability": "steward_execution",
            "available_in": "V8",
            "side_effects_started": false,
            "report": report,
            "scheduler": runtime::StewardScheduler::projection(),
            "projection": runtime::MissionControlRuntime::projection(self.runtime_services()),
        })
    }

    pub(crate) fn steward_scheduler_handoff(&self, steward_id: &str) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.steward_handoff_summary",
            "ok": true,
            "handoff": runtime::StewardScheduler::handoff_summary(steward_id),
            "runtime_report": runtime::global_steward_runtime_service().report(steward_id).ok(),
        })
    }

    pub(crate) fn steward_detail(&self, steward_id: &str) -> Result<serde_json::Value, String> {
        let steward = runtime::global_steward_runtime_service()
            .get(steward_id)
            .ok_or_else(|| format!("steward not found: {steward_id}"))?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "steward": steward,
            "report": runtime::global_steward_runtime_service().report(steward_id)?,
        }))
    }

    pub(crate) fn start_steward(
        &self,
        request: StartMissionStewardHttpRequest,
    ) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": false,
            "status": "capability_unavailable",
            "capability": "steward_execution",
            "available_in": "V8",
            "side_effects_started": false,
            "request": {
                "mission_id": request.mission_id,
                "root_session_id": request.root_session_id,
                "profile_id": request.profile_id,
                "objective": request.objective,
            },
            "stewards": runtime::global_steward_runtime_service().projection(),
        }))
    }

    pub(crate) fn start_session(
        &self,
        request: StartMissionSessionHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let session = self
            .mission()
            .start_session(runtime::StartMissionSessionRequest {
                title: request.title,
                session_id: request.session_id,
            })?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "session": session,
            "mission": self.mission_projection(),
        }))
    }

    pub(crate) fn session_detail(&self, session_id: &str) -> Result<serde_json::Value, String> {
        let session = self
            .mission()
            .get_session(session_id)
            .ok_or_else(|| format!("mission session not found: {session_id}"))?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission.session",
            "session": session,
            "mission": self.mission_projection(),
        }))
    }

    pub(crate) fn switch_session(&self, session_id: &str) -> Result<serde_json::Value, String> {
        self.command_value(self.mission().switch_session(session_id)?)
    }

    pub(crate) fn background_session(&self, session_id: &str) -> Result<serde_json::Value, String> {
        self.command_value(self.mission().background_session(session_id)?)
    }

    pub(crate) fn pause_session(&self, session_id: &str) -> Result<serde_json::Value, String> {
        self.command_value(self.mission().pause_session(session_id)?)
    }

    pub(crate) fn close_session(&self, session_id: &str) -> Result<serde_json::Value, String> {
        self.command_value(self.mission().close_session(session_id)?)
    }

    pub(crate) fn attach_team(
        &self,
        session_id: &str,
        request: AttachMissionTeamHttpRequest,
    ) -> Result<serde_json::Value, String> {
        self.command_value(self.mission().attach_team(session_id, request.team_id)?)
    }

    pub(crate) fn attach_agent(
        &self,
        session_id: &str,
        request: AttachMissionAgentHttpRequest,
    ) -> Result<serde_json::Value, String> {
        self.command_value(self.mission().attach_agent(session_id, request.agent_id)?)
    }

    pub(crate) fn start_team_runtime(
        &self,
        session_id: &str,
        request: StartMissionTeamRuntimeHttpRequest,
    ) -> Result<serde_json::Value, String> {
        if self.mission().get_session(session_id).is_none() {
            return Err(format!("mission session not found: {session_id}"));
        }
        if request.objective.trim().is_empty() {
            return Err("team objective must not be empty".to_string());
        }
        let compile = runtime::ExecutionGraphCompiler.compile(runtime::ExecutionCompileRequest {
            objective: request.objective,
            payload_ref: format!("mission_session:{session_id}"),
            target: runtime::RuntimeCompileTarget::TeamGraph,
            resource_scopes: vec![format!("session:{session_id}")],
        });
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": false,
            "status": "capability_unavailable",
            "capability": "collaborate",
            "available_in": "V4",
            "compile_error": compile.err().map(|error| error.to_string()),
            "requested_model": request.model,
            "requested_execution_mode": request.execution_mode,
            "approval_id": request.approval_id,
            "mission": self.mission_projection(),
        }))
    }

    pub(crate) fn submit_approval(
        &self,
        request: SubmitMissionApprovalHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let approval = self.runtime_services().approval_queue().submit(
            runtime::SubmitGlobalApprovalRequest {
                source: request.source,
                action: request.action,
                summary: request.summary,
                risk: request.risk,
                evidence_refs: request.evidence_refs,
                timeout_policy: request.timeout_policy,
            },
        )?;
        Ok(serde_json::json!({
            "envelope": self.approval_command_contract(),
            "ok": true,
            "approval": approval,
            "approvals": self.runtime_services().approval_queue().projection(),
        }))
    }

    pub(crate) fn decide_approval(
        &self,
        approval_id: &str,
        request: DecideMissionApprovalHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let receipt =
            self.runtime_services()
                .approval_queue()
                .decide(runtime::GlobalApprovalDecision {
                    approval_id: approval_id.to_string(),
                    approved: request.approved,
                    decided_by: request.decided_by,
                    reason: request.reason,
                })?;
        Ok(serde_json::json!({
            "envelope": self.approval_command_contract(),
            "ok": true,
            "receipt": receipt,
            "approvals": self.runtime_services().approval_queue().projection(),
        }))
    }

    pub(crate) fn add_relation(
        &self,
        request: AddMissionRelationHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let relation = self.relation_graph().add_relation(
            request.from_session_id,
            request.to_session_id,
            request.kind,
            request.summary,
            request.evidence_refs,
        )?;
        Ok(serde_json::json!({
            "envelope": self.relation_command_contract(),
            "ok": true,
            "relation": relation,
            "relations": self.relation_graph().projection(),
        }))
    }

    pub(crate) fn upsert_proxy(
        &self,
        request: UpsertMissionProxyHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let proxy = self.relation_graph().upsert_proxy(runtime::SessionProxy {
            session_id: request.session_id,
            summary: request.summary,
            evidence_refs: request.evidence_refs,
            decisions: request.decisions,
            open_questions: request.open_questions,
            updated_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        })?;
        Ok(serde_json::json!({
            "envelope": self.relation_command_contract(),
            "ok": true,
            "proxy": proxy,
            "relations": self.relation_graph().projection(),
        }))
    }

    pub(crate) fn route_command(
        &self,
        request: RouteMissionCommandHttpRequest,
    ) -> Result<serde_json::Value, String> {
        if self
            .mission()
            .get_session(&request.from_session_id)
            .is_none()
        {
            return Err(format!(
                "mission session not found: {}",
                request.from_session_id
            ));
        }
        let receipt = self.relation_graph().route(runtime::SessionRouteCommand {
            from_session_id: request.from_session_id.clone(),
            target_ref: request.target_ref.clone(),
            command: request.command.clone(),
        });
        let routed = route_mission_command_receipt(self.mission(), &receipt, &request.command)?;
        Ok(serde_json::json!({
            "envelope": self.relation_command_contract(),
            "ok": true,
            "receipt": receipt,
            "routed": routed,
            "mission": self.mission_projection(),
            "relations": self.relation_graph().projection(),
        }))
    }

    pub(crate) fn session_inbox(&self, session_id: &str) -> Result<serde_json::Value, String> {
        if self.mission().get_session(session_id).is_none() {
            return Err(format!("mission session not found: {session_id}"));
        }
        let commands = self.mission().list_session_commands(session_id);
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "session_id": session_id,
            "commands": commands,
            "mission": self.mission_projection(),
        }))
    }

    pub(crate) fn session_command_detail(
        &self,
        session_id: &str,
        command_id: &str,
    ) -> Result<serde_json::Value, String> {
        let command = self
            .mission()
            .get_session_command(command_id)
            .ok_or_else(|| format!("mission session command not found: {command_id}"))?;
        if command.target_session_id != session_id {
            return Err(format!(
                "command {command_id} does not belong to session {session_id}"
            ));
        }
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "command": command,
            "mission": self.mission_projection(),
        }))
    }

    pub(crate) fn consume_session_command(
        &self,
        session_id: &str,
        command_id: &str,
        request: ConsumeMissionSessionCommandHttpRequest,
    ) -> Result<serde_json::Value, String> {
        if request.mode == MissionSessionCommandConsumeMode::StartTurn {
            return Err(
                "start_turn command consumption must be handled by the async mission route"
                    .to_string(),
            );
        }
        let command = self
            .mission()
            .claim_session_command(session_id, command_id)?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "mode": request.mode,
            "actor_id": request.actor_id,
            "reason": request.reason,
            "command": command,
            "mission": self.mission_projection(),
        }))
    }

    pub(crate) fn cancel_session_command(
        &self,
        session_id: &str,
        command_id: &str,
    ) -> Result<serde_json::Value, String> {
        let command = self
            .mission()
            .cancel_session_command(session_id, command_id)?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "command": command,
            "mission": self.mission_projection(),
        }))
    }

    pub(crate) fn retry_session_command(
        &self,
        session_id: &str,
        command_id: &str,
    ) -> Result<serde_json::Value, String> {
        let command = self
            .mission()
            .retry_session_command(session_id, command_id)?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "command": command,
            "mission": self.mission_projection(),
        }))
    }

    pub(crate) fn tick_steward(
        &self,
        steward_id: &str,
        request: TickMissionStewardHttpRequest,
    ) -> Result<serde_json::Value, String> {
        if runtime::global_steward_runtime_service()
            .get(steward_id)
            .is_none()
        {
            return Err(format!("steward not found: {steward_id}"));
        }
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": false,
            "status": "capability_unavailable",
            "capability": "steward_execution",
            "available_in": "V4",
            "side_effects_started": false,
            "steward_id": steward_id,
            "request": {
                "action": request.action,
                "summary": request.summary,
                "risk": request.risk,
                "requested_tool": request.requested_tool,
                "requires_write": request.requires_write,
                "is_critical_operation": request.is_critical_operation,
                "evidence_refs": request.evidence_refs,
                "timeout_policy": request.timeout_policy,
            },
            "stewards": runtime::global_steward_runtime_service().projection(),
        }))
    }

    pub(crate) fn pause_steward(&self, steward_id: &str) -> Result<serde_json::Value, String> {
        self.steward_status_value(runtime::global_steward_runtime_service().pause(steward_id)?)
    }

    pub(crate) fn resume_steward(&self, steward_id: &str) -> Result<serde_json::Value, String> {
        self.steward_status_value(runtime::global_steward_runtime_service().resume(steward_id)?)
    }

    pub(crate) fn interrupt_steward(
        &self,
        steward_id: &str,
        request: InterruptMissionStewardHttpRequest,
    ) -> Result<serde_json::Value, String> {
        self.steward_status_value(
            runtime::global_steward_runtime_service().interrupt(
                steward_id,
                request
                    .reason
                    .unwrap_or_else(|| "interrupted from Mission API".to_string()),
            )?,
        )
    }

    pub(crate) fn takeover_steward(&self, steward_id: &str) -> Result<serde_json::Value, String> {
        let report = runtime::global_steward_runtime_service().takeover(steward_id)?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "report": report,
            "stewards": runtime::global_steward_runtime_service().projection(),
        }))
    }

    pub(crate) fn steward_report(&self, steward_id: &str) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "report": runtime::global_steward_runtime_service().report(steward_id)?,
        }))
    }

    fn steward_status_value(
        &self,
        steward: runtime::StewardSession,
    ) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "steward": steward,
            "stewards": runtime::global_steward_runtime_service().projection(),
        }))
    }

    fn command_value(
        &self,
        receipt: runtime::MissionCommandReceipt,
    ) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "receipt": receipt,
            "mission": self.mission_projection(),
        }))
    }
}

fn team_capability_unavailable(
    service: &MissionService,
    kind: &str,
    team_id: &str,
    capability: &str,
    available_in: &str,
) -> serde_json::Value {
    serde_json::json!({
        "envelope": service.session_control_contract(),
        "kind": kind,
        "ok": false,
        "status": "capability_unavailable",
        "capability": capability,
        "available_in": available_in,
        "side_effects_started": false,
        "team_id": team_id,
    })
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn route_mission_command_receipt(
    mission: &runtime::MissionRuntime,
    receipt: &runtime::SessionRouteReceipt,
    command: &str,
) -> Result<serde_json::Value, String> {
    if let Some(session_id) = &receipt.resolved_session_id {
        let command = mission.enqueue_session_command(
            &receipt.from_session_id,
            session_id,
            command.to_string(),
        )?;
        return Ok(serde_json::json!({
            "kind": "mission.session_command",
            "command": command,
        }));
    }
    if let Some(agent_id) = &receipt.resolved_agent_id {
        if runtime::global_agent_lifecycle_service()
            .get(agent_id)
            .is_none()
        {
            return Err(format!("route target agent not found: {agent_id}"));
        }
        return Ok(serde_json::json!({
            "kind": "mission.agent_command",
            "ok": false,
            "status": "capability_unavailable",
            "capability": "agent_execution",
            "available_in": "V5",
            "agent_id": agent_id,
            "command": {
                "kind": "input",
                "from_session_id": receipt.from_session_id,
                "target_ref": receipt.target_ref,
                "text": command,
            },
        }));
    }
    Err(format!("route target not resolved: {}", receipt.target_ref))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scoped_mission_service() -> MissionService {
        MissionService::new().with_runtime_services(
            runtime::RuntimeServices::in_memory().expect("workspace-scoped runtime services"),
        )
    }

    #[test]
    fn start_mission_team_runtime_request_rejects_unknown_fields() {
        let error =
            serde_json::from_value::<StartMissionTeamRuntimeHttpRequest>(serde_json::json!({
                "objective": "review the implementation",
                "execution_mode": "manual_mailbox",
                "execution_pattern": "manual_mailbox",
            }))
            .expect_err("unknown fields must be rejected");

        assert!(
            error
                .to_string()
                .contains("unknown field `execution_pattern`"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn mission_service_projects_runtime_control_surfaces() {
        let service = scoped_mission_service();
        let session_id = format!("mission-service-test-{}", uuid::Uuid::new_v4());
        let started = service
            .start_session(StartMissionSessionHttpRequest {
                title: "verify mission service".to_string(),
                session_id: Some(session_id.clone()),
            })
            .expect("start session");

        assert_eq!(started["ok"], true);
        assert_eq!(started["envelope"]["service"], "mission");
        assert_eq!(
            started["session"]["session_id"].as_str(),
            Some(session_id.as_str())
        );
        assert!(started["mission"]["sessions"]
            .as_array()
            .expect("mission sessions")
            .iter()
            .any(|session| session["session_id"].as_str() == Some(session_id.as_str())));
        let detail = service.session_detail(&session_id).expect("session detail");
        assert_eq!(
            detail["session"]["session_id"].as_str(),
            Some(session_id.as_str())
        );

        let background = service
            .background_session(&session_id)
            .expect("background session");
        assert_eq!(background["receipt"]["status"], "accepted");
        let projection = service.projection();
        assert_eq!(projection["mission"]["kind"], "mission.runtime");
        assert_eq!(projection["mission"]["schema_version"], 2);
        assert!(projection["mission"]["conflict_projection"].is_null());
        assert_eq!(
            projection["mission"]["capability_projection"]["name"],
            "cowd-runtime-capability-catalog"
        );
        assert_eq!(
            service.approvals()["approvals"]["kind"],
            "runtime.global_approvals"
        );
        assert_eq!(
            service.relations()["relations"]["kind"],
            "runtime.session_relations"
        );
        assert_eq!(
            service.conflicts()["conflicts"]["kind"],
            "runtime.conflicts"
        );
    }

    #[test]
    fn mission_service_rejects_team_execution_until_runner_executor_is_available() {
        let service = scoped_mission_service();
        let session_id = format!("mission-task-outcome-{}", uuid::Uuid::new_v4());
        service
            .start_session(StartMissionSessionHttpRequest {
                title: "task outcome".to_string(),
                session_id: Some(session_id.clone()),
            })
            .expect("session");
        let started = service
            .start_team_runtime(
                &session_id,
                StartMissionTeamRuntimeHttpRequest {
                    objective: "answer one delegated question".to_string(),
                    model: None,
                    execution_mode: MissionTeamExecutionMode::ManualMailbox,
                    approval_id: None,
                },
            )
            .expect("team");
        assert_eq!(started["ok"], false);
        assert_eq!(started["status"], "capability_unavailable");
        assert_eq!(started["capability"], "collaborate");
        assert_eq!(started["available_in"], "V5");
    }

    #[test]
    fn mission_service_rejects_steward_execution_without_mutating_globals() {
        let service = scoped_mission_service();
        let approvals_before = service.runtime_services().approval_queue().projection()["count"]
            .as_u64()
            .expect("approval projection count");

        let response = service.tick_all_stewards();

        assert_eq!(response["ok"], false);
        assert_eq!(response["status"], "capability_unavailable");
        assert_eq!(response["capability"], "steward_execution");
        assert_eq!(response["available_in"], "V8");
        assert_eq!(
            service.runtime_services().approval_queue().projection()["count"]
                .as_u64()
                .expect("approval projection count"),
            approvals_before
        );
    }
}
