use std::collections::BTreeSet;

use ai_kernel::strategy::{decide_strategy, StrategyInput};
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
pub(crate) struct StartMissionTeamRuntimeHttpRequest {
    pub(crate) objective: String,
    #[serde(default)]
    pub(crate) model: Option<String>,
    #[serde(default)]
    pub(crate) execution_mode: MissionTeamExecutionMode,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MissionTeamExecutionMode {
    #[default]
    ProviderInProcess,
    ProcessJsonl,
    RegisterOnly,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SubmitMissionApprovalHttpRequest {
    pub(crate) source: runtime::ApprovalSource,
    pub(crate) action: String,
    pub(crate) summary: String,
    pub(crate) risk: ai_kernel::core::TaskRisk,
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
    pub(crate) risk: Option<ai_kernel::core::TaskRisk>,
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
            owner: "0.9.376 Mission Runtime service boundary",
        }
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
            self.approval_command_contract(),
            self.relation_command_contract(),
        ]
    }

    pub(crate) fn projection(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.projection_contract(),
            "mission": runtime::global_mission_runtime().projection(),
            "stewards": runtime::global_steward_runtime_service().projection(),
        })
    }

    pub(crate) fn approvals(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.approval_projection_contract(),
            "approvals": runtime::global_approval_queue().projection(),
        })
    }

    pub(crate) fn relations(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.relation_projection_contract(),
            "relations": runtime::global_session_relation_graph().projection(),
        })
    }

    pub(crate) fn stewards(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "stewards": runtime::global_steward_runtime_service().projection(),
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
        let steward = runtime::global_steward_runtime_service().start(
            runtime::StartStewardRuntimeRequest {
                mission_id: request.mission_id,
                root_session_id: request.root_session_id,
                profile_id: request.profile_id,
                objective: request.objective,
            },
        )?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "steward": steward,
            "stewards": runtime::global_steward_runtime_service().projection(),
        }))
    }

    pub(crate) fn start_session(
        &self,
        request: StartMissionSessionHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let session = runtime::global_mission_runtime().start_session(
            runtime::StartMissionSessionRequest {
                title: request.title,
                session_id: request.session_id,
            },
        )?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "session": session,
            "mission": runtime::global_mission_runtime().projection(),
        }))
    }

    pub(crate) fn session_detail(&self, session_id: &str) -> Result<serde_json::Value, String> {
        let session = runtime::global_mission_runtime()
            .get_session(session_id)
            .ok_or_else(|| format!("mission session not found: {session_id}"))?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission.session",
            "session": session,
            "mission": runtime::global_mission_runtime().projection(),
        }))
    }

    pub(crate) fn switch_session(&self, session_id: &str) -> Result<serde_json::Value, String> {
        self.command_value(runtime::global_mission_runtime().switch_session(session_id)?)
    }

    pub(crate) fn background_session(&self, session_id: &str) -> Result<serde_json::Value, String> {
        self.command_value(runtime::global_mission_runtime().background_session(session_id)?)
    }

    pub(crate) fn pause_session(&self, session_id: &str) -> Result<serde_json::Value, String> {
        self.command_value(runtime::global_mission_runtime().pause_session(session_id)?)
    }

    pub(crate) fn close_session(&self, session_id: &str) -> Result<serde_json::Value, String> {
        self.command_value(runtime::global_mission_runtime().close_session(session_id)?)
    }

    pub(crate) fn attach_team(
        &self,
        session_id: &str,
        request: AttachMissionTeamHttpRequest,
    ) -> Result<serde_json::Value, String> {
        self.command_value(
            runtime::global_mission_runtime().attach_team(session_id, request.team_id)?,
        )
    }

    pub(crate) fn attach_agent(
        &self,
        session_id: &str,
        request: AttachMissionAgentHttpRequest,
    ) -> Result<serde_json::Value, String> {
        self.command_value(
            runtime::global_mission_runtime().attach_agent(session_id, request.agent_id)?,
        )
    }

    pub(crate) fn start_team_runtime(
        &self,
        session_id: &str,
        request: StartMissionTeamRuntimeHttpRequest,
    ) -> Result<serde_json::Value, String> {
        if runtime::global_mission_runtime()
            .get_session(session_id)
            .is_none()
        {
            return Err(format!("mission session not found: {session_id}"));
        }
        if request.objective.trim().is_empty() {
            return Err("team objective must not be empty".to_string());
        }
        let strategy = decide_strategy(&StrategyInput::from_prompt(request.objective.clone()));
        let decision =
            runtime::CollaborationTemplateMatcher::default().decide(&request.objective, &strategy);
        let team = runtime::global_team_runtime_service().start_with_agent_spawner(
            runtime::StartTeamRuntimeRequest {
                session_id: session_id.to_string(),
                objective: request.objective.clone(),
                collaboration_decision: decision,
            },
            |agent_request| {
                spawn_lifecycle_agent_for_team(
                    &agent_request,
                    request.model.as_deref(),
                    request.execution_mode,
                )
            },
        )?;
        runtime::global_mission_runtime().attach_team(session_id, team.team_id.clone())?;
        for agent in &team.agents {
            if let Some(agent_id) = &agent.agent_id {
                runtime::global_mission_runtime().attach_agent(session_id, agent_id.clone())?;
            }
        }
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "team": team,
            "mission": runtime::global_mission_runtime().projection(),
        }))
    }

    pub(crate) fn submit_approval(
        &self,
        request: SubmitMissionApprovalHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let approval =
            runtime::global_approval_queue().submit(runtime::SubmitGlobalApprovalRequest {
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
            "approvals": runtime::global_approval_queue().projection(),
        }))
    }

    pub(crate) fn decide_approval(
        &self,
        approval_id: &str,
        request: DecideMissionApprovalHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let receipt = runtime::global_approval_queue().decide(runtime::GlobalApprovalDecision {
            approval_id: approval_id.to_string(),
            approved: request.approved,
            decided_by: request.decided_by,
            reason: request.reason,
        })?;
        Ok(serde_json::json!({
            "envelope": self.approval_command_contract(),
            "ok": true,
            "receipt": receipt,
            "approvals": runtime::global_approval_queue().projection(),
        }))
    }

    pub(crate) fn add_relation(
        &self,
        request: AddMissionRelationHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let relation = runtime::global_session_relation_graph().add_relation(
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
            "relations": runtime::global_session_relation_graph().projection(),
        }))
    }

    pub(crate) fn upsert_proxy(
        &self,
        request: UpsertMissionProxyHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let proxy =
            runtime::global_session_relation_graph().upsert_proxy(runtime::SessionProxy {
                session_id: request.session_id,
                summary: request.summary,
                evidence_refs: request.evidence_refs,
                decisions: request.decisions,
                open_questions: request.open_questions,
                updated_at_ms: current_time_ms(),
            })?;
        Ok(serde_json::json!({
            "envelope": self.relation_command_contract(),
            "ok": true,
            "proxy": proxy,
            "relations": runtime::global_session_relation_graph().projection(),
        }))
    }

    pub(crate) fn route_command(
        &self,
        request: RouteMissionCommandHttpRequest,
    ) -> Result<serde_json::Value, String> {
        if runtime::global_mission_runtime()
            .get_session(&request.from_session_id)
            .is_none()
        {
            return Err(format!(
                "mission session not found: {}",
                request.from_session_id
            ));
        }
        let receipt =
            runtime::global_session_relation_graph().route(runtime::SessionRouteCommand {
                from_session_id: request.from_session_id.clone(),
                target_ref: request.target_ref.clone(),
                command: request.command.clone(),
            });
        let routed = route_mission_command_receipt(&receipt, &request.command)?;
        Ok(serde_json::json!({
            "envelope": self.relation_command_contract(),
            "ok": true,
            "receipt": receipt,
            "routed": routed,
            "mission": runtime::global_mission_runtime().projection(),
            "relations": runtime::global_session_relation_graph().projection(),
        }))
    }

    pub(crate) fn session_inbox(&self, session_id: &str) -> Result<serde_json::Value, String> {
        if runtime::global_mission_runtime()
            .get_session(session_id)
            .is_none()
        {
            return Err(format!("mission session not found: {session_id}"));
        }
        let commands = runtime::global_mission_runtime().list_session_commands(session_id);
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "session_id": session_id,
            "commands": commands,
            "mission": runtime::global_mission_runtime().projection(),
        }))
    }

    pub(crate) fn session_command_detail(
        &self,
        session_id: &str,
        command_id: &str,
    ) -> Result<serde_json::Value, String> {
        let command = runtime::global_mission_runtime()
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
            "mission": runtime::global_mission_runtime().projection(),
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
        let command =
            runtime::global_mission_runtime().claim_session_command(session_id, command_id)?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "mode": request.mode,
            "actor_id": request.actor_id,
            "reason": request.reason,
            "command": command,
            "mission": runtime::global_mission_runtime().projection(),
        }))
    }

    pub(crate) fn cancel_session_command(
        &self,
        session_id: &str,
        command_id: &str,
    ) -> Result<serde_json::Value, String> {
        let command =
            runtime::global_mission_runtime().cancel_session_command(session_id, command_id)?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "command": command,
            "mission": runtime::global_mission_runtime().projection(),
        }))
    }

    pub(crate) fn retry_session_command(
        &self,
        session_id: &str,
        command_id: &str,
    ) -> Result<serde_json::Value, String> {
        let command =
            runtime::global_mission_runtime().retry_session_command(session_id, command_id)?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "command": command,
            "mission": runtime::global_mission_runtime().projection(),
        }))
    }

    pub(crate) fn tick_steward(
        &self,
        steward_id: &str,
        request: TickMissionStewardHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let record = runtime::global_steward_runtime_service().tick(
            steward_id,
            runtime::TickStewardRuntimeRequest {
                action: request.action,
                summary: request.summary,
                risk: request.risk.unwrap_or(ai_kernel::core::TaskRisk::Low),
                requested_tool: request.requested_tool,
                requires_write: request.requires_write,
                is_critical_operation: request.is_critical_operation,
                evidence_refs: request.evidence_refs,
                timeout_policy: request
                    .timeout_policy
                    .unwrap_or(runtime::ApprovalTimeoutPolicy::Pending),
            },
        )?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "decision": record,
            "stewards": runtime::global_steward_runtime_service().projection(),
            "approvals": runtime::global_approval_queue().projection(),
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
            "mission": runtime::global_mission_runtime().projection(),
        }))
    }
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn spawn_lifecycle_agent_for_team(
    request: &runtime::StartTeamRuntimeAgentRequest,
    model: Option<&str>,
    execution_mode: MissionTeamExecutionMode,
) -> Result<runtime::AgentSnapshot, String> {
    match execution_mode {
        MissionTeamExecutionMode::ProviderInProcess => {
            spawn_provider_lifecycle_agent_for_team(request, model)
        }
        MissionTeamExecutionMode::ProcessJsonl => spawn_process_jsonl_lifecycle_agent_for_team(
            request,
            model,
            resolve_team_process_jsonl_spec()?,
        ),
        MissionTeamExecutionMode::RegisterOnly => register_lifecycle_agent_for_team(request, model),
    }
}

fn spawn_provider_lifecycle_agent_for_team(
    request: &runtime::StartTeamRuntimeAgentRequest,
    model: Option<&str>,
) -> Result<runtime::AgentSnapshot, String> {
    runtime::spawn_provider_agent(
        build_spawn_request_for_team(
            request,
            model,
            runtime::AgentExecutionBackendKind::InProcess,
            None,
        )?,
        runtime::StaticToolExecutor::new(),
    )
}

fn spawn_process_jsonl_lifecycle_agent_for_team(
    request: &runtime::StartTeamRuntimeAgentRequest,
    model: Option<&str>,
    spec: runtime::AgentProcessJsonlSpec,
) -> Result<runtime::AgentSnapshot, String> {
    runtime::spawn_provider_agent(
        build_spawn_request_for_team(
            request,
            model,
            runtime::AgentExecutionBackendKind::ProcessJsonl,
            Some(spec),
        )?,
        runtime::StaticToolExecutor::new(),
    )
}

fn build_spawn_request_for_team(
    request: &runtime::StartTeamRuntimeAgentRequest,
    model: Option<&str>,
    backend: runtime::AgentExecutionBackendKind,
    process_jsonl: Option<runtime::AgentProcessJsonlSpec>,
) -> Result<runtime::SpawnAgentRequest, String> {
    let subagent_type = runtime::normalize_subagent_type(Some(&request.role_id));
    let prompt = team_role_prompt(request);
    Ok(runtime::SpawnAgentRequest {
        description: format!("{}: {}", request.role_id, request.responsibility),
        prompt,
        subagent_type: Some(subagent_type.clone()),
        name: Some(format!("{}-{}", request.team_id, request.role_id)),
        model: Some(runtime::resolve_agent_model(model)),
        system_prompt: runtime::build_agent_system_prompt(&subagent_type)?,
        allowed_tools: BTreeSet::new(),
        tool_definitions: Vec::new(),
        permission_policy: runtime::PermissionPolicy::new(runtime::PermissionMode::ReadOnly),
        max_iterations: runtime::DEFAULT_AGENT_MAX_ITERATIONS,
        store_dir: None,
        backend,
        process_jsonl,
    })
}

fn resolve_team_process_jsonl_spec() -> Result<runtime::AgentProcessJsonlSpec, String> {
    let command = std::env::var("COWD_AGENT_PROCESS_JSONL_COMMAND").map_err(|_| {
        "COWD_AGENT_PROCESS_JSONL_COMMAND is required for process-jsonl team execution".to_string()
    })?;
    let args = match std::env::var("COWD_AGENT_PROCESS_JSONL_ARGS") {
        Ok(raw) if !raw.trim().is_empty() => {
            serde_json::from_str::<Vec<String>>(&raw).map_err(|error| {
                format!("COWD_AGENT_PROCESS_JSONL_ARGS must be a JSON string array: {error}")
            })?
        }
        _ => Vec::new(),
    };
    Ok(runtime::AgentProcessJsonlSpec {
        command,
        args,
        cwd: None,
        env: Default::default(),
    })
}

fn register_lifecycle_agent_for_team(
    request: &runtime::StartTeamRuntimeAgentRequest,
    model: Option<&str>,
) -> Result<runtime::AgentSnapshot, String> {
    let agent_id = format!("agent-{}", uuid::Uuid::new_v4());
    let agent_dir = runtime::cowd_dirs::user_agents_dir();
    std::fs::create_dir_all(&agent_dir).map_err(|error| error.to_string())?;
    let output_file = agent_dir.join(format!("{agent_id}.md"));
    let manifest_file = agent_dir.join(format!("{agent_id}.json"));
    let prompt = team_role_prompt(request);
    let created_at = current_time_ms().to_string();
    std::fs::write(
        &output_file,
        format!(
            "# Agent Task\n\n- id: {agent_id}\n- role: {}\n- status: queued\n\n## Prompt\n\n{prompt}\n",
            request.role_id
        ),
    )
    .map_err(|error| error.to_string())?;
    let snapshot = runtime::AgentSnapshot {
        agent_id,
        name: format!("{}-{}", request.team_id, request.role_id),
        description: format!("{}: {}", request.role_id, request.responsibility),
        subagent_type: Some(request.role_id.clone()),
        model: Some(model.unwrap_or(runtime::DEFAULT_AGENT_MODEL).to_string()),
        status: "queued".to_string(),
        backend: runtime::AgentExecutionBackendKind::InProcess,
        output_file: output_file.display().to_string(),
        manifest_file: manifest_file.display().to_string(),
        created_at: created_at.clone(),
        started_at: Some(created_at),
        completed_at: None,
        lane_events: Vec::new(),
        current_blocker: None,
        derived_state: "queued_for_executor".to_string(),
        error: None,
    };
    let manifest = serde_json::to_string_pretty(&snapshot).map_err(|error| error.to_string())?;
    std::fs::write(&manifest_file, manifest).map_err(|error| error.to_string())?;
    runtime::global_agent_lifecycle_service()
        .register_started(snapshot.clone(), runtime::CancellationToken::new());
    Ok(snapshot)
}

fn team_role_prompt(request: &runtime::StartTeamRuntimeAgentRequest) -> String {
    format!(
        "Mission session: {}\nTeam: {}\nObjective: {}\nRole: {}\nResponsibility: {}\nAllowed tools: {}\nEvidence duties: {}\n\nWork only on this delegated role. Produce a concise terminal report with evidence references, blockers, and changed artifacts if any.\n",
        request.session_id,
        request.team_id,
        request.objective,
        request.role_id,
        request.responsibility,
        request.allowed_tools.join(", "),
        request.evidence_duties.join(", ")
    )
}

fn route_mission_command_receipt(
    receipt: &runtime::SessionRouteReceipt,
    command: &str,
) -> Result<serde_json::Value, String> {
    if let Some(session_id) = &receipt.resolved_session_id {
        let command = runtime::global_mission_runtime().enqueue_session_command(
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
        let agent_receipt = runtime::global_agent_lifecycle_service().command(
            agent_id,
            runtime::AgentExecutionCommandKind::Input,
            Some(serde_json::json!({
                "from_session_id": receipt.from_session_id,
                "target_ref": receipt.target_ref,
                "text": command,
            })),
        )?;
        return Ok(serde_json::json!({
            "kind": "mission.agent_command",
            "receipt": agent_receipt,
        }));
    }
    Err(format!("route target not resolved: {}", receipt.target_ref))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mission_service_projects_runtime_control_surfaces() {
        let service = MissionService::new();
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
            started["mission"]["active_session_id"].as_str(),
            Some(session_id.as_str())
        );

        let background = service
            .background_session(&session_id)
            .expect("background session");
        assert_eq!(background["receipt"]["status"], "accepted");
        assert_eq!(service.projection()["mission"]["kind"], "mission.runtime");
        assert_eq!(
            service.approvals()["approvals"]["kind"],
            "runtime.global_approvals"
        );
        assert_eq!(
            service.relations()["relations"]["kind"],
            "runtime.session_relations"
        );
    }
}
