use harness_contract::core::ExecutionMode;
use serde_json::{json, Value};

use crate::execution_core::reflexion::ReflexionRecord;
use crate::orchestration::planner::{
    plan_runtime_collaboration_decision, RuntimeOrchestrationPlan,
};
use crate::orchestration::request::{RuntimeOrchestrationAction, RuntimeOrchestrationRequest};
use crate::{global_team_runtime_service, StartTeamRuntimeRequest};

#[must_use]
pub fn execute_orchestration_request(
    request: &RuntimeOrchestrationRequest,
    plan: &RuntimeOrchestrationPlan,
    decision_status: &str,
) -> (Option<String>, Value) {
    let mode = plan.execution_decision.recommended_mode;
    match request.action {
        RuntimeOrchestrationAction::RequestRewooEvidence => (
            Some("ready".to_string()),
            json!({
                "type": "rewoo_executor",
                "mode": mode.as_str(),
                "status": "ready",
                "execution_fidelity": "runtime_owned_evidence_packet",
                "engine": {
                    "name": "RewooExecutor",
                    "owned_by": "runtime",
                    "dispatch_surface": "conversation_tool_scheduler"
                },
                "next_step": "dispatch evidence steps through the conversation runtime tool scheduler when a tool executor is attached; keep evidence refs and summaries as the solver input",
                "plan": plan.rewoo_plan,
                "tool_dag": plan.tool_dag,
                "observation_packet": plan.rewoo_plan.synthetic_result(),
                "expected_events": ["ToolStart", "ToolComplete", "ContextEnvelope", "RunModelTelemetry"],
            }),
        ),
        RuntimeOrchestrationAction::RequestParallelTools => (
            Some("ready".to_string()),
            json!({
                "type": "tool_dag_executor",
                "mode": mode.as_str(),
                "status": "ready",
                "execution_fidelity": "runtime_owned_executable_dag",
                "engine": {
                    "name": "ToolDagExecutor",
                    "owned_by": "runtime",
                    "dispatch_surface": "conversation_tool_scheduler"
                },
                "next_step": "execute independent read-only nodes as one scheduler batch; serialize write/destructive nodes through permission gates",
                "tool_dag": plan.tool_dag,
                "schedule": plan.tool_dag.safety_summary.schedule,
                "expected_events": ["ToolStart", "ToolComplete", "ToolExecuted"],
            }),
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
            (
                Some("running".to_string()),
                json!({
                    "type": "team_runtime",
                    "mode": mode.as_str(),
                    "status": "running",
                    "team": team,
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
