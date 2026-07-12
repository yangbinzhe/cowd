use serde::Deserialize;

use super::{service_envelope, MissionService, ServiceEnvelope};

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct StartMissionSessionHttpRequest {
    pub(crate) title: String,
    #[serde(default)]
    pub(crate) session_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateMissionScheduleHttpRequest {
    pub(crate) mission_id: String,
    pub(crate) target_session_id: String,
    pub(crate) objective: String,
    pub(crate) trigger: harness_contract::mission::ScheduleTrigger,
    #[serde(default = "default_schedule_autonomy_profile")]
    pub(crate) autonomy_profile: String,
    #[serde(default = "default_schedule_permission_lease")]
    pub(crate) permission_lease: String,
    #[serde(default = "default_schedule_priority")]
    pub(crate) priority: u8,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateMissionScheduleHttpRequest {
    pub(crate) expected_revision: u64,
    #[serde(default)]
    pub(crate) objective: Option<String>,
    #[serde(default)]
    pub(crate) trigger: Option<harness_contract::mission::ScheduleTrigger>,
    #[serde(default)]
    pub(crate) autonomy_profile: Option<String>,
    #[serde(default)]
    pub(crate) permission_lease: Option<String>,
    #[serde(default)]
    pub(crate) priority: Option<u8>,
}

fn default_schedule_autonomy_profile() -> String {
    "assisted".to_string()
}

fn default_schedule_permission_lease() -> String {
    "read_only".to_string()
}

const fn default_schedule_priority() -> u8 {
    64
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StartMissionTeamRuntimeHttpRequest {
    pub(crate) objective: String,
    #[serde(default)]
    pub(crate) model: Option<String>,
    #[serde(default)]
    pub(crate) backend: Option<runtime::AgentBackendKind>,
    #[serde(default)]
    pub(crate) approval_id: Option<String>,
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
pub(crate) struct InterpretMissionCommandHttpRequest {
    pub(crate) current_session_id: String,
    pub(crate) command_text: String,
    #[serde(default)]
    pub(crate) target_ref: Option<String>,
    #[serde(default)]
    pub(crate) dispatch_mode: Option<runtime::SessionDispatchMode>,
    #[serde(default)]
    pub(crate) allow_background: Option<bool>,
    #[serde(default)]
    pub(crate) execute: bool,
}

impl MissionService {
    pub(crate) fn new() -> Self {
        Self {
            label: "mission",
            owner: "0.9.380 Mission Runtime service boundary",
            runtime_port: None,
        }
    }

    pub(crate) fn with_runtime_port(mut self, runtime_port: runtime::MissionRuntimePort) -> Self {
        self.runtime_port = Some(runtime_port);
        self
    }

    fn runtime(&self) -> &runtime::MissionRuntimePort {
        self.runtime_port
            .as_ref()
            .expect("MissionService requires MissionRuntimePort")
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
            "mission": self.runtime().projection(),
        })
    }

    pub(crate) fn mission_control(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "projection": self.runtime().control_projection(),
        })
    }

    pub(crate) async fn execute_mission_control_command(
        &self,
        command: runtime::MissionControlCommand,
    ) -> serde_json::Value {
        let receipt = self.runtime().execute_control(command).await;
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
            "projection": self.runtime().control_projection(),
        })
    }

    pub(crate) async fn interpret_mission_command(
        &self,
        request: InterpretMissionCommandHttpRequest,
    ) -> serde_json::Value {
        let interpretation = runtime::MissionCommandInterpreter::interpret(
            runtime::MissionCommandInterpretRequest {
                current_session_id: request.current_session_id,
                command_text: request.command_text,
                target_ref: request.target_ref,
                dispatch_mode: request.dispatch_mode,
                allow_background: request.allow_background,
            },
        );
        let execution = if request.execute {
            Some(
                self.runtime()
                    .submit_interpretation(interpretation.clone())
                    .await,
            )
        } else {
            None
        };
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.command_interpretation",
            "ok": interpretation.status == "interpreted"
                && execution
                    .as_ref()
                    .and_then(|result| result.get("ok"))
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(true),
            "interpretation": interpretation,
            "execution": execution,
            "projection": self.runtime().control_projection(),
        })
    }

    pub(crate) fn team_execution_plan(&self, team_id: &str) -> Result<serde_json::Value, String> {
        let team = self.runtime().team_projection(team_id)?;
        let graph = self.runtime().team_graph(team_id)?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.team_execution_plan",
            "ok": true,
            "team": team,
            "graph": graph,
        }))
    }

    pub(crate) fn collaboration_runs(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.collaboration_runs",
            "ok": true,
            "projection": self.runtime().team_projection_json(),
        })
    }

    pub(crate) fn collaboration_run(&self, team_id: &str) -> Result<serde_json::Value, String> {
        let run = self.runtime().team_projection(team_id)?;
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
        let receipt = self.runtime().cancel_team(team_id).await?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.team_cancel",
            "ok": true,
            "receipt": receipt,
        }))
    }

    pub(crate) fn agent_mission_events(&self, agent_id: &str) -> serde_json::Value {
        let data = self.runtime().agent_events(agent_id);
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.agent_events",
            "ok": true,
            "agent_id": agent_id,
            "events": data["events"],
            "run": data["run"],
        })
    }

    pub(crate) fn team_mission_evidence(&self, team_id: &str) -> serde_json::Value {
        let data = self.runtime().team_evidence(team_id);
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.team_evidence",
            "ok": true,
            "team_id": team_id,
            "events": data["events"],
            "tasks": data["tasks"],
            "team": data["team"],
            "evidence": data["evidence"],
        })
    }

    pub(crate) fn approvals(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.approval_projection_contract(),
            "approvals": self.runtime().approvals_projection(),
        })
    }

    pub(crate) fn relations(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.relation_projection_contract(),
            "relations": self.runtime().relations_projection(),
        })
    }

    pub(crate) fn conflicts(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.conflict_projection_contract(),
            "conflicts": self.runtime().conflicts_projection(),
        })
    }

    pub(crate) fn start_session(
        &self,
        request: StartMissionSessionHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let session_id = request.session_id.ok_or_else(|| {
            "Mission session IDs must be allocated by the unified session boundary".to_string()
        })?;
        let session = self
            .runtime()
            .start_session(runtime::StartMissionSessionRequest {
                title: request.title,
                session_id: Some(session_id),
            })?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "session": session,
            "mission": self.runtime().projection(),
        }))
    }

    pub(crate) fn schedules(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "schedules": self.runtime().schedule_projection(),
            "policy": self.runtime().schedule_policy(),
        })
    }

    pub(crate) fn create_schedule(
        &self,
        request: CreateMissionScheduleHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let schedule = self
            .runtime()
            .create_schedule(runtime::CreateMissionScheduleRequest {
                mission_id: request.mission_id,
                target_session_id: request.target_session_id,
                objective: request.objective,
                trigger: request.trigger,
                autonomy_profile: request.autonomy_profile,
                permission_lease: request.permission_lease,
                priority: request.priority,
            })?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "schedule": schedule,
            "schedules": self.runtime().schedule_projection(),
            "policy": self.runtime().schedule_policy(),
        }))
    }

    pub(crate) async fn tick_schedules(&self) -> Result<serde_json::Value, String> {
        let report = self.runtime().dispatch_due_schedules().await?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": report.failed.is_empty(),
            "report": report,
            "schedules": self.runtime().schedule_projection(),
            "policy": self.runtime().schedule_policy(),
        }))
    }

    pub(crate) fn pause_schedule(&self, schedule_id: &str) -> Result<serde_json::Value, String> {
        let schedule = self.runtime().pause_schedule(schedule_id)?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "schedule": schedule,
        }))
    }

    pub(crate) fn resume_schedule(&self, schedule_id: &str) -> Result<serde_json::Value, String> {
        let schedule = self.runtime().resume_schedule(schedule_id)?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "schedule": schedule,
        }))
    }

    pub(crate) fn update_schedule(
        &self,
        schedule_id: &str,
        request: UpdateMissionScheduleHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let schedule = self.runtime().update_schedule(
            schedule_id,
            runtime::UpdateMissionScheduleRequest {
                expected_revision: request.expected_revision,
                objective: request.objective,
                trigger: request.trigger,
                autonomy_profile: request.autonomy_profile,
                permission_lease: request.permission_lease,
                priority: request.priority,
            },
        )?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "schedule": schedule,
        }))
    }

    pub(crate) fn session_detail(&self, session_id: &str) -> Result<serde_json::Value, String> {
        let session = self
            .runtime()
            .session(session_id)
            .ok_or_else(|| format!("mission session not found: {session_id}"))?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission.session",
            "session": session,
            "mission": self.runtime().projection(),
        }))
    }

    pub(crate) async fn switch_session(&self, session_id: &str) -> serde_json::Value {
        self.session_transition_command(session_id, runtime::MissionControlAction::SwitchSession)
            .await
    }

    pub(crate) async fn background_session(&self, session_id: &str) -> serde_json::Value {
        self.session_transition_command(
            session_id,
            runtime::MissionControlAction::BackgroundSession,
        )
        .await
    }

    pub(crate) async fn pause_session(&self, session_id: &str) -> serde_json::Value {
        self.session_transition_command(session_id, runtime::MissionControlAction::PauseSession)
            .await
    }

    pub(crate) async fn close_session(&self, session_id: &str) -> serde_json::Value {
        self.session_transition_command(session_id, runtime::MissionControlAction::CloseSession)
            .await
    }

    pub(crate) async fn start_team_runtime(
        &self,
        session_id: &str,
        request: StartMissionTeamRuntimeHttpRequest,
    ) -> Result<serde_json::Value, String> {
        if self.runtime().session(session_id).is_none() {
            return Err(format!("mission session not found: {session_id}"));
        }
        if request.objective.trim().is_empty() {
            return Err("team objective must not be empty".to_string());
        }
        let team_id = format!("team-{}", uuid::Uuid::new_v4());
        let model_lease = request.model.unwrap_or_default();
        let backend_constraint = request.backend.map(|backend| match backend {
            runtime::AgentBackendKind::InProcess => "backend:in_process".to_string(),
            runtime::AgentBackendKind::ProcessJsonl => "backend:process_jsonl".to_string(),
        });
        let roles = vec![
            harness_contract::team::TeamRoleSpec {
                role_id: "executor".to_string(),
                responsibility: "produce an evidence-backed solution for the requested objective"
                    .to_string(),
                required_capabilities: vec!["analysis".to_string()],
                allowed_tools: vec!["read_file".to_string(), "search".to_string()],
                acceptance: vec!["propose a complete result with evidence".to_string()],
                evidence_duties: vec!["source evidence".to_string()],
            },
            harness_contract::team::TeamRoleSpec {
                role_id: "reviewer".to_string(),
                responsibility: "independently review the solution for gaps, conflicts, and risks"
                    .to_string(),
                required_capabilities: vec!["review".to_string()],
                allowed_tools: vec!["read_file".to_string(), "search".to_string()],
                acceptance: vec!["report verified risks and unresolved issues".to_string()],
                evidence_duties: vec!["review evidence".to_string()],
            },
        ];
        let team = self
            .runtime()
            .start_team(runtime::StartTeamRequest {
                team_id,
                session_id: session_id.to_string(),
                objective: request.objective,
                template_id: harness_contract::team::TeamTemplateId::ExecuteReview,
                roles,
                role_dependencies: vec![runtime::TeamRoleDependency {
                    from_role_id: "executor".to_string(),
                    to_role_id: "reviewer".to_string(),
                }],
                lift_input: runtime::CollaborationLiftInput {
                    independent_work_items: 2,
                    domain_count: 2,
                    shared_write_scope: false,
                    review_required: true,
                    provider_healthy: true,
                    budget_allows_parallelism: true,
                    requested_parallelism: 2,
                },
                permission_lease: "read_only".to_string(),
                model_lease,
                backend_constraint,
            })
            .await?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "status": team.status,
            "team": team,
            "requested_backend": request.backend,
            "approval_id": request.approval_id,
            "mission": self.runtime().projection(),
        }))
    }

    pub(crate) fn submit_approval(
        &self,
        request: SubmitMissionApprovalHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let approval = self
            .runtime()
            .submit_approval(runtime::SubmitGlobalApprovalRequest {
                source: request.source,
                action: request.action,
                summary: request.summary,
                risk: request.risk,
                evidence_refs: request.evidence_refs,
                timeout_policy: request.timeout_policy,
            })?;
        Ok(serde_json::json!({
            "envelope": self.approval_command_contract(),
            "ok": true,
            "approval": approval,
            "approvals": self.runtime().approvals_projection(),
        }))
    }

    pub(crate) fn decide_approval(
        &self,
        approval_id: &str,
        request: DecideMissionApprovalHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let receipt = self
            .runtime()
            .decide_approval(runtime::GlobalApprovalDecision {
                approval_id: approval_id.to_string(),
                approved: request.approved,
                decided_by: request.decided_by,
                reason: request.reason,
            })?;
        Ok(serde_json::json!({
            "envelope": self.approval_command_contract(),
            "ok": true,
            "receipt": receipt,
            "approvals": self.runtime().approvals_projection(),
        }))
    }

    pub(crate) fn add_relation(
        &self,
        request: AddMissionRelationHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let relation = self.runtime().add_relation(
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
            "relations": self.runtime().relations_projection(),
        }))
    }

    pub(crate) fn upsert_proxy(
        &self,
        request: UpsertMissionProxyHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let proxy = self.runtime().upsert_proxy(runtime::SessionProxy {
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
            "relations": self.runtime().relations_projection(),
        }))
    }

    async fn session_transition_command(
        &self,
        session_id: &str,
        action: runtime::MissionControlAction,
    ) -> serde_json::Value {
        self.execute_mission_control_command(runtime::MissionControlCommand {
            target: runtime::MissionControlCommandTarget::Session {
                session_id: session_id.to_string(),
            },
            action,
            actor: Some("gateway_mission_session_route".to_string()),
            payload: serde_json::Value::Null,
            evidence_refs: Vec::new(),
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scoped_mission_service() -> MissionService {
        MissionService::new().with_runtime_port(runtime::MissionRuntimePort::new(
            runtime::RuntimeServices::in_memory().expect("workspace-scoped runtime services"),
        ))
    }

    #[test]
    fn start_mission_team_runtime_request_rejects_unknown_fields() {
        let error =
            serde_json::from_value::<StartMissionTeamRuntimeHttpRequest>(serde_json::json!({
                "objective": "review the implementation",
                "backend": "in_process",
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

    #[tokio::test]
    async fn mission_service_projects_runtime_control_surfaces() {
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

        let background = service.background_session(&session_id).await;
        assert_eq!(background["receipt"]["status"], "executed");
        let projection = service.projection();
        assert_eq!(projection["mission"]["kind"], "mission.runtime");
        assert_eq!(projection["mission"]["schema_version"], 3);
        assert_eq!(
            projection["mission"]["conflict_projection"]["kind"],
            "runtime.conflicts"
        );
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

    #[tokio::test]
    async fn mission_service_creates_a_runner_owned_team_graph() {
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
                    backend: Some(runtime::AgentBackendKind::InProcess),
                    approval_id: None,
                },
            )
            .await
            .expect("team");
        assert_eq!(started["ok"], true);
        assert!(started["team"]["graph_id"].as_str().is_some());
        assert!(started["mission"]["team_projection"]["teams"]
            .as_array()
            .is_some_and(|teams| !teams.is_empty()));
    }

    #[test]
    fn mission_service_exposes_runtime_owned_schedule_policy() {
        let service = scoped_mission_service();
        let projection = service.schedules();
        assert_eq!(projection["policy"]["enabled"], true);
        assert!(projection["policy"]["tick_interval_ms"].as_u64().is_some());
    }
}
