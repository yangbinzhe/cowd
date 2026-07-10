use harness_contract::core::ExecutionModifier;
use harness_contract::core::ExecutionPattern;
use std::collections::BTreeSet;

use crate::execution_core::RuntimeCompileTarget;
use serde_json::{json, Value};

use crate::execution_core::reflexion::ReflexionRecord;
use crate::orchestration::planner::RuntimeOrchestrationPlan;
use crate::orchestration::request::{RuntimeOrchestrationAction, RuntimeOrchestrationRequest};
use crate::tool_host::{
    execute_tool_dag_with_host, RuntimeActionExecutionReceipt, RuntimeExecutionHost,
};
use crate::{
    global_agent_lifecycle_service, global_mission_runtime, global_steward_runtime_service,
    global_team_runtime_service, prepare_agent_job, AgentExecutionBackendKind, AutonomyProfileId,
    CrossSessionMessage, PermissionMode, PermissionPolicy, SessionExecutionPlane,
    SpawnAgentRequest, StartStewardRuntimeRequest, StartTeamRuntimeRequest,
    DEFAULT_AGENT_MAX_ITERATIONS,
};

#[must_use]
pub fn execute_orchestration_request(
    request: &RuntimeOrchestrationRequest,
    plan: &RuntimeOrchestrationPlan,
    decision_status: &str,
    execution_host: Option<&dyn RuntimeExecutionHost>,
) -> (Option<String>, Value) {
    let mode = plan.execution_decision.pattern();
    if !plan.execution_decision.executable {
        return (
            Some("rejected".to_string()),
            json!({
                "type": "resource_contract_guard",
                "status": "rejected",
                "blocked_reasons": &plan.execution_decision.blocked_reasons,
                "action": request.action,
                "side_effects_started": false,
            }),
        );
    }
    if !compile_target_allows(plan.execution_decision.compile_target, request.action) {
        return (
            Some("rejected".to_string()),
            json!({
                "type": "compile_target_guard",
                "status": "rejected",
                "compile_target": plan.execution_decision.compile_target,
                "action": request.action,
                "side_effects_started": false,
            }),
        );
    }
    if !modifier_contract_allows(&plan.execution_decision, request.action) {
        return (
            Some("rejected".to_string()),
            json!({
                "type": "modifier_contract_guard",
                "status": "rejected",
                "modifiers": plan.execution_decision.modifiers(),
                "action": request.action,
                "side_effects_started": false,
            }),
        );
    }
    match request.action {
        RuntimeOrchestrationAction::RequestRewooEvidence => execute_tool_dag_action(
            "request_rewoo_evidence",
            "rewoo_executor",
            mode,
            &plan.tool_dag,
            execution_host,
            Some(json!(plan.rewoo_plan.synthetic_result())),
        ),
        RuntimeOrchestrationAction::RequestParallelTools => execute_tool_dag_action(
            "request_parallel_tools",
            "tool_dag_executor",
            mode,
            &plan.tool_dag,
            execution_host,
            None,
        ),
        RuntimeOrchestrationAction::RequestDeliberation => (
            Some("ready".to_string()),
            json!({
                "type": "deliberation_executor",
                "pattern": mode.as_str(),
                "status": "ready",
                "execution_fidelity": "runtime_owned_deliberation_packet",
                "engine": {
                    "name": "DeliberationExecutor",
                    "owned_by": "runtime",
                    "dispatch_surface": "model_candidate_generation"
                },
                "next_step": "generate candidates, critique assumptions, merge the strongest path, and record unresolved risks as a decision artifact",
                "plan": plan.deliberation_plan,
                "candidate_slots": plan.deliberation_plan.candidate_count,
                "decision_artifact": {
                    "required_sections": ["candidates", "critique", "merged_decision", "risks"],
                    "human_readable": true
                },
            }),
        ),
        RuntimeOrchestrationAction::RequestReflexionRetry => (
            Some("ready".to_string()),
            json!({
                "type": "reflexion_executor",
                "pattern": mode.as_str(),
                "status": "ready",
                "execution_fidelity": "runtime_owned_reflexion_guard",
                "engine": {
                    "name": "ReflexionExecutor",
                    "owned_by": "runtime",
                    "dispatch_surface": "self_regulation"
                },
                "next_step": "switch strategy before retrying; stop after bounded retry budget or answer from checked evidence with residual risks",
                "record": ReflexionRecord::low_novelty_tool_loop("model requested reflexion retry"),
                "stop_conditions": ["retry_budget_exhausted", "no_new_evidence", "quality_gate_passed"],
            }),
        ),
        RuntimeOrchestrationAction::PlanOnly => (
            None,
            json!({
                "type": "plan_only",
                "pattern": mode.as_str(),
                "execution_patterns": plan.pattern_catalog.summary(),
                "rewoo_candidate": plan.rewoo_plan,
                "tool_dag_candidate": plan.tool_dag,
            }),
        ),
        RuntimeOrchestrationAction::RequestTeam => {
            execute_team_request(request, plan, mode, decision_status, execution_host)
        }
        RuntimeOrchestrationAction::RequestRiskGate => (
            None,
            json!({
                "type": "risk_gate",
                "pattern": mode.as_str(),
                "status": decision_status,
                "required_context": ["approval_policy", "risk_reason", "permission_scope"],
                "next_step": "request approval or provide a lower-risk alternative before execution",
            }),
        ),
        RuntimeOrchestrationAction::RequestSubagent => {
            execute_agent_lifecycle_request(request, mode, decision_status, "request_subagent")
        }
        RuntimeOrchestrationAction::RequestVerification => {
            execute_agent_lifecycle_request(request, mode, decision_status, "request_verification")
        }
        RuntimeOrchestrationAction::RequestBackgroundReview => {
            execute_background_review_request(request, mode, decision_status)
        }
        RuntimeOrchestrationAction::RequestSessionLink => {
            execute_session_link_request(request, mode, decision_status)
        }
    }
}

fn modifier_contract_allows(
    decision: &crate::execution_core::RuntimeExecutionDecision,
    action: RuntimeOrchestrationAction,
) -> bool {
    match action {
        RuntimeOrchestrationAction::RequestParallelTools => {
            decision.modifiers().contains(&ExecutionModifier::Parallel)
        }
        RuntimeOrchestrationAction::RequestTeam => {
            decision.modifiers().contains(&ExecutionModifier::Parallel)
        }
        RuntimeOrchestrationAction::RequestBackgroundReview
        | RuntimeOrchestrationAction::RequestSessionLink => decision
            .modifiers()
            .contains(&ExecutionModifier::Background),
        _ => true,
    }
}

fn compile_target_allows(target: RuntimeCompileTarget, action: RuntimeOrchestrationAction) -> bool {
    use RuntimeCompileTarget as Target;
    use RuntimeOrchestrationAction as Action;

    match target {
        Target::InlineModel => matches!(action, Action::PlanOnly | Action::RequestRiskGate),
        Target::EvidenceGraph => matches!(
            action,
            Action::PlanOnly
                | Action::RequestParallelTools
                | Action::RequestRewooEvidence
                | Action::RequestVerification
                | Action::RequestReflexionRetry
                | Action::RequestRiskGate
        ),
        Target::ExecutionGraph => matches!(
            action,
            Action::PlanOnly
                | Action::RequestSubagent
                | Action::RequestVerification
                | Action::RequestParallelTools
                | Action::RequestReflexionRetry
                | Action::RequestRiskGate
        ),
        Target::DeliberationGraph => matches!(
            action,
            Action::PlanOnly
                | Action::RequestDeliberation
                | Action::RequestVerification
                | Action::RequestReflexionRetry
                | Action::RequestRiskGate
        ),
        Target::TeamGraph => matches!(
            action,
            Action::PlanOnly
                | Action::RequestTeam
                | Action::RequestVerification
                | Action::RequestReflexionRetry
                | Action::RequestRiskGate
        ),
        Target::MissionGraph => matches!(
            action,
            Action::PlanOnly
                | Action::RequestBackgroundReview
                | Action::RequestSessionLink
                | Action::RequestVerification
                | Action::RequestRiskGate
        ),
    }
}

fn execute_tool_dag_action(
    action: &str,
    executor_type: &str,
    mode: ExecutionPattern,
    dag: &crate::execution_core::tool_dag::ToolDagPlan,
    execution_host: Option<&dyn RuntimeExecutionHost>,
    observation_packet: Option<Value>,
) -> (Option<String>, Value) {
    let receipt = match execution_host {
        Some(host) => execute_tool_dag_with_host(action, dag, host),
        None => RuntimeActionExecutionReceipt::blocked_missing_executor(action, dag),
    };
    let status = receipt.status.clone();
    let status_override = match status.as_str() {
        "executed" | "degraded_permission_blocked" | "degraded_empty_dag" => {
            Some("executed".to_string())
        }
        "blocked_missing_executor" | "blocked_permission" => Some(status.clone()),
        "failed" => Some("failed".to_string()),
        _ => Some(status.clone()),
    };
    let mut detail = json!({
        "type": executor_type,
        "pattern": mode.as_str(),
        "status": status,
        "execution_fidelity": "runtime_owned_executable_dag",
        "engine": {
            "name": "RuntimeActionExecutor",
            "owned_by": "runtime",
            "dispatch_surface": "runtime_execution_host"
        },
        "tool_dag": dag,
        "schedule": dag.safety_summary.schedule,
        "receipt": receipt,
    });
    if let Some(packet) = observation_packet {
        if let Some(object) = detail.as_object_mut() {
            object.insert("observation_packet".to_string(), packet);
        }
    }
    (status_override, detail)
}

fn execute_team_request(
    request: &RuntimeOrchestrationRequest,
    plan: &RuntimeOrchestrationPlan,
    mode: ExecutionPattern,
    decision_status: &str,
    host: Option<&dyn RuntimeExecutionHost>,
) -> (Option<String>, Value) {
    if decision_status != "accepted" {
        return (
            None,
            json!({
                "type": "team_runtime",
                "pattern": mode.as_str(),
                "status": decision_status,
                "reason": "team runtime was not started because validation did not accept the request",
                "required_context": ["session_id"],
            }),
        );
    }
    let Some(session_id) = request
        .session_id
        .as_deref()
        .filter(|value| !value.is_empty())
    else {
        return (
            Some("rejected".to_string()),
            json!({
                "type": "team_runtime",
                "pattern": mode.as_str(),
                "status": "rejected",
                "reason": "request_team requires session_id to attach a real TeamRuntime",
                "required_context": ["session_id"],
            }),
        );
    };
    if let Some(host) = host {
        if let Some(result) = host.start_runtime_team(request, &plan.collaboration_decision) {
            return match result {
                Ok(value) => (Some("running".to_string()), value),
                Err(error) => (
                    Some("failed".to_string()),
                    json!({
                        "type": "team_runtime",
                        "pattern": mode.as_str(),
                        "status": "failed",
                        "error": error,
                        "execution_fidelity": "runtime_owned_gateway_adapter",
                    }),
                ),
            };
        }
    }
    match global_team_runtime_service().start(StartTeamRuntimeRequest {
        session_id: session_id.to_string(),
        objective: request.intent.clone(),
        collaboration_decision: plan.collaboration_decision.clone(),
    }) {
        Ok(team) => {
            let team_id = team.team_id.clone();
            let collaboration_run = global_team_runtime_service()
                .collaboration_run(&team_id)
                .ok();
            (
                Some("running".to_string()),
                json!({
                    "type": "team_runtime",
                    "pattern": mode.as_str(),
                    "status": "running",
                    "team": team,
                    "collaboration_run": collaboration_run,
                    "control_actions": ["inspect", "synthesis", "handoff", "cancel", "pause"],
                    "expected_events": ["team.started", "team.input_appended", "team.handoff_requested", "team.cancelled"],
                    "event_refs": [format!("team:{team_id}")],
                }),
            )
        }
        Err(error) => (
            Some("failed".to_string()),
            json!({
                "type": "team_runtime",
                "pattern": mode.as_str(),
                "status": "failed",
                "error": error,
            }),
        ),
    }
}

fn execute_agent_lifecycle_request(
    request: &RuntimeOrchestrationRequest,
    mode: ExecutionPattern,
    decision_status: &str,
    action: &str,
) -> (Option<String>, Value) {
    if decision_status != "accepted" {
        return lifecycle_not_started(action, mode, decision_status);
    }
    let template = match action {
        "request_verification" => Some("verifier".to_string()),
        _ => request.template_hint.clone(),
    };
    let permission_mode = if action == "request_verification"
        || !request.constraints.requires_write.unwrap_or(false)
    {
        PermissionMode::ReadOnly
    } else {
        PermissionMode::WorkspaceWrite
    };
    let description = match action {
        "request_verification" => format!("Verify: {}", request.intent),
        _ => format!("Subagent: {}", request.intent),
    };
    let prompt = agent_prompt_for(action, request);
    let job = match prepare_agent_job(SpawnAgentRequest {
        description,
        prompt,
        subagent_type: template,
        name: Some(action.replace("request_", "")),
        model: None,
        system_prompt: Vec::new(),
        allowed_tools: BTreeSet::new(),
        tool_definitions: Vec::new(),
        permission_policy: PermissionPolicy::new(permission_mode),
        max_iterations: DEFAULT_AGENT_MAX_ITERATIONS,
        store_dir: None,
        backend: AgentExecutionBackendKind::InProcess,
        process_jsonl: None,
    }) {
        Ok(job) => job,
        Err(error) => {
            return (
                Some("failed".to_string()),
                json!({
                    "type": "runtime_orchestration_result",
                    "pattern": mode.as_str(),
                    "action": action,
                    "status": "failed",
                    "execution_fidelity": "runtime_owned_agent_lifecycle",
                    "error": error,
                }),
            );
        }
    };
    let manifest = job.manifest.clone();
    global_agent_lifecycle_service()
        .register_started(manifest.clone(), job.cancellation_token.clone());
    global_agent_lifecycle_service().record_event(
        &manifest.agent_id,
        "agent.waiting_executor",
        "Runtime orchestration created a lifecycle job; provider execution may be attached by the runtime host.",
    );
    let attach_status = request
        .session_id
        .as_deref()
        .filter(|session_id| !session_id.trim().is_empty())
        .map(|session_id| {
            match global_mission_runtime().attach_agent(session_id, &manifest.agent_id) {
                Ok(receipt) => json!({"status": "attached", "receipt": receipt}),
                Err(error) => json!({"status": "failed", "error": error}),
            }
        });
    let status = if attach_status
        .as_ref()
        .and_then(|value| value.get("status"))
        .and_then(Value::as_str)
        == Some("failed")
    {
        "running_degraded"
    } else {
        "running"
    };
    let agent_id = manifest.agent_id.clone();
    (
        Some(status.to_string()),
        json!({
            "type": "runtime_orchestration_result",
            "pattern": mode.as_str(),
            "action": action,
            "status": status,
            "execution_fidelity": if action == "request_verification" {
                "runtime_owned_verification_lifecycle"
            } else {
                "runtime_owned_agent_lifecycle"
            },
            "agent_id": agent_id,
            "agent": manifest,
            "session_id": request.session_id,
            "attach_status": attach_status,
            "event_refs": [format!("agent:{agent_id}")],
            "control_actions": ["inspect", "cancel", "request_report"],
            "evidence_refs": request.evidence_refs,
        }),
    )
}

fn execute_background_review_request(
    request: &RuntimeOrchestrationRequest,
    mode: ExecutionPattern,
    decision_status: &str,
) -> (Option<String>, Value) {
    if decision_status != "accepted" {
        return lifecycle_not_started("request_background_review", mode, decision_status);
    }
    let mission_id = request
        .session_id
        .as_deref()
        .filter(|session_id| !session_id.trim().is_empty())
        .map(|session_id| format!("mission:{session_id}"))
        .unwrap_or_else(|| format!("mission-background-{}", uuid::Uuid::new_v4()));
    match global_steward_runtime_service().start(StartStewardRuntimeRequest {
        mission_id,
        root_session_id: request.session_id.clone(),
        profile_id: AutonomyProfileId::Stewarded,
        objective: request.intent.clone(),
    }) {
        Ok(steward) => (Some("running".to_string()), {
            let steward_id = steward.steward_id.clone();
            json!({
            "type": "runtime_orchestration_result",
            "pattern": mode.as_str(),
            "action": "request_background_review",
            "status": "running",
            "execution_fidelity": "runtime_owned_steward_lifecycle",
            "steward_id": steward.steward_id,
            "steward": steward,
            "watch_scope": {
                "session_id": request.session_id,
                "evidence_refs": request.evidence_refs,
            },
            "event_refs": [format!("steward:{steward_id}")],
            "control_actions": ["pause", "resume", "takeover", "cancel", "request_report"],
            })
        }),
        Err(error) => (
            Some("failed".to_string()),
            json!({
                "type": "runtime_orchestration_result",
                "pattern": mode.as_str(),
                "action": "request_background_review",
                "status": "failed",
                "execution_fidelity": "runtime_owned_steward_lifecycle",
                "error": error,
            }),
        ),
    }
}

fn execute_session_link_request(
    request: &RuntimeOrchestrationRequest,
    mode: ExecutionPattern,
    decision_status: &str,
) -> (Option<String>, Value) {
    if decision_status != "accepted" {
        return lifecycle_not_started("request_session_link", mode, decision_status);
    }
    let Some(session_id) = request
        .session_id
        .as_deref()
        .filter(|session_id| !session_id.trim().is_empty())
    else {
        return (
            Some("rejected".to_string()),
            json!({
                "type": "runtime_orchestration_result",
                "pattern": mode.as_str(),
                "action": "request_session_link",
                "status": "rejected",
                "execution_fidelity": "runtime_owned_session_bridge",
                "reason": "request_session_link requires session_id",
            }),
        );
    };
    let target_ref = request
        .template_hint
        .as_deref()
        .or(request.surface.as_deref())
        .unwrap_or("primary");
    let receipt = SessionExecutionPlane::bridge(CrossSessionMessage {
        from_session_id: session_id.to_string(),
        target_ref: target_ref.to_string(),
        command: request.intent.clone(),
        actor: Some("runtime_orchestrate".to_string()),
        evidence_refs: request.evidence_refs.clone(),
    });
    let status = match receipt.status.as_str() {
        "routed" => "routed",
        "failed" => "failed",
        _ => "rejected",
    };
    (
        Some(status.to_string()),
        json!({
            "type": "runtime_orchestration_result",
            "pattern": mode.as_str(),
            "action": "request_session_link",
            "status": status,
            "execution_fidelity": "runtime_owned_session_bridge",
            "bridge": receipt,
            "event_refs": [format!("session:{}", session_id)],
            "control_actions": ["inspect", "dispatch_pending", "recover_route"],
        }),
    )
}

fn lifecycle_not_started(
    action: &str,
    mode: ExecutionPattern,
    decision_status: &str,
) -> (Option<String>, Value) {
    (
        None,
        json!({
            "type": "runtime_orchestration_result",
            "pattern": mode.as_str(),
            "action": action,
            "status": decision_status,
            "execution_fidelity": "runtime_owned_lifecycle_guard",
            "reason": "runtime lifecycle was not started because validation did not accept the request",
        }),
    )
}

fn agent_prompt_for(action: &str, request: &RuntimeOrchestrationRequest) -> String {
    let mut prompt = String::new();
    match action {
        "request_verification" => prompt.push_str("Verify the current work and report concrete risks, missing tests, and evidence-backed conclusions.\n\n"),
        _ => prompt.push_str("Execute the delegated runtime subtask and report progress, evidence, and residual risk.\n\n"),
    }
    prompt.push_str("Intent:\n");
    prompt.push_str(&request.intent);
    if !request.evidence_refs.is_empty() {
        prompt.push_str("\n\nEvidence refs:\n");
        for evidence_ref in &request.evidence_refs {
            prompt.push_str("- ");
            prompt.push_str(evidence_ref);
            prompt.push('\n');
        }
    }
    prompt
}

#[must_use]
pub fn guidance_for(action: &RuntimeOrchestrationAction) -> String {
    match action {
        RuntimeOrchestrationAction::PlanOnly => {
            "Review the runtime plan, then request the lightest sufficient action.".to_string()
        }
        RuntimeOrchestrationAction::RequestParallelTools
        | RuntimeOrchestrationAction::RequestRewooEvidence => {
            "Use the returned evidence refs and summaries; avoid repeated overlapping reads."
                .to_string()
        }
        RuntimeOrchestrationAction::RequestDeliberation => {
            "Compare candidates, state tradeoffs, and merge a defensible recommendation."
                .to_string()
        }
        RuntimeOrchestrationAction::RequestReflexionRetry => {
            "Switch strategy before retrying; do not repeat the failed tool path.".to_string()
        }
        _ => "Track runtime events and continue from the orchestration result.".to_string(),
    }
}
