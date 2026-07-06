use harness_contract::core::ExecutionMode;
use serde_json::{json, Value};

use crate::execution_core::reflexion::ReflexionRecord;
use crate::orchestration::planner::{
    plan_runtime_collaboration_decision, RuntimeOrchestrationPlan,
};
use crate::orchestration::request::{RuntimeOrchestrationAction, RuntimeOrchestrationRequest};
use crate::tool_host::{
    execute_tool_dag_with_host, RuntimeActionExecutionReceipt, RuntimeToolExecutionHost,
};
use crate::{global_team_runtime_service, StartTeamRuntimeRequest};

#[must_use]
pub fn execute_orchestration_request(
    request: &RuntimeOrchestrationRequest,
    plan: &RuntimeOrchestrationPlan,
    decision_status: &str,
    tool_host: Option<&dyn RuntimeToolExecutionHost>,
) -> (Option<String>, Value) {
    let mode = plan.execution_decision.recommended_mode;
    match request.action {
        RuntimeOrchestrationAction::RequestRewooEvidence => execute_tool_dag_action(
            "request_rewoo_evidence",
            "rewoo_executor",
            mode,
            &plan.tool_dag,
            tool_host,
            Some(json!(plan.rewoo_plan.synthetic_result())),
        ),
        RuntimeOrchestrationAction::RequestParallelTools => execute_tool_dag_action(
            "request_parallel_tools",
            "tool_dag_executor",
            mode,
            &plan.tool_dag,
            tool_host,
            None,
        ),
        RuntimeOrchestrationAction::RequestDeliberation => (
            Some("ready".to_string()),
            json!({
                "type": "deliberation_executor",
                "mode": mode.as_str(),
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
                "mode": mode.as_str(),
                "status": "ready",
                "execution_fidelity": "runtime_owned_reflexion_guard",
                "engine": {
                    "name": "ReflexionExecutor",
                    "owned_by": "runtime",
                    "dispatch_surface": "turn_supervisor"
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
                "mode": mode.as_str(),
                "execution_modes": plan.mode_catalog.summary(),
                "rewoo_candidate": plan.rewoo_plan,
                "tool_dag_candidate": plan.tool_dag,
            }),
        ),
        RuntimeOrchestrationAction::RequestTeam => {
            execute_team_request(request, mode, decision_status)
        }
        RuntimeOrchestrationAction::RequestRiskGate => (
            None,
            json!({
                "type": "risk_gate",
                "mode": mode.as_str(),
                "status": decision_status,
                "required_context": ["approval_policy", "risk_reason", "permission_scope"],
                "next_step": "request approval or provide a lower-risk alternative before execution",
            }),
        ),
        RuntimeOrchestrationAction::RequestSubagent
        | RuntimeOrchestrationAction::RequestVerification
        | RuntimeOrchestrationAction::RequestBackgroundReview
        | RuntimeOrchestrationAction::RequestSessionLink => (
            None,
            json!({
                "type": "runtime_lifecycle_request",
                "mode": mode.as_str(),
                "action": request.action,
                "template_hint": request.template_hint,
                "execution_target": "runtime-owned team/session/approval lifecycle",
                "status": decision_status,
            }),
        ),
    }
}

fn execute_tool_dag_action(
    action: &str,
    executor_type: &str,
    mode: ExecutionMode,
    dag: &crate::execution_core::tool_dag::ToolDagPlan,
    tool_host: Option<&dyn RuntimeToolExecutionHost>,
    observation_packet: Option<Value>,
) -> (Option<String>, Value) {
    let receipt = match tool_host {
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
        "mode": mode.as_str(),
        "status": status,
        "execution_fidelity": "runtime_owned_executable_dag",
        "engine": {
            "name": "RuntimeActionExecutor",
            "owned_by": "runtime",
            "dispatch_surface": "runtime_tool_host"
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
    mode: ExecutionMode,
    decision_status: &str,
) -> (Option<String>, Value) {
    if decision_status != "accepted" {
        return (
            None,
            json!({
                "type": "team_runtime",
                "mode": mode.as_str(),
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
                "mode": mode.as_str(),
                "status": "rejected",
                "reason": "request_team requires session_id to attach a real TeamRuntime",
                "required_context": ["session_id"],
            }),
        );
    };
    let collaboration_decision = plan_runtime_collaboration_decision(&request.intent);
    match global_team_runtime_service().start(StartTeamRuntimeRequest {
        session_id: session_id.to_string(),
        objective: request.intent.clone(),
        collaboration_decision,
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
                    "mode": mode.as_str(),
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
                "mode": mode.as_str(),
                "status": "failed",
                "error": error,
            }),
        ),
    }
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
