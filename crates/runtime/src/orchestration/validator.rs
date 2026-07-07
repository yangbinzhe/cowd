use harness_contract::core::ExecutionMode;
use serde_json::json;

use crate::orchestration::request::{RuntimeOrchestrationAction, RuntimeOrchestrationRequest};
use crate::orchestration::result::RuntimeOrchestrationDecision;

#[must_use]
pub fn validate_request(
    request: &RuntimeOrchestrationRequest,
    recommended_mode: &ExecutionMode,
) -> RuntimeOrchestrationDecision {
    let mut policy_gates = Vec::new();
    let mut status = match request.action {
        RuntimeOrchestrationAction::RequestTeam
        | RuntimeOrchestrationAction::RequestSubagent
        | RuntimeOrchestrationAction::RequestVerification
        | RuntimeOrchestrationAction::RequestBackgroundReview
        | RuntimeOrchestrationAction::RequestSessionLink => "accepted",
        RuntimeOrchestrationAction::RequestRiskGate => "needs_approval",
        _ => "planned",
    }
    .to_string();
    if matches!(request.action, RuntimeOrchestrationAction::RequestRiskGate) {
        policy_gates.push("risk_gate_requested".to_string());
    }
    if request
        .constraints
        .risk
        .as_deref()
        .is_some_and(|risk| matches!(risk, "high" | "critical"))
    {
        if status != "rejected" {
            status = "needs_approval".to_string();
        }
        policy_gates.push("risk_requires_approval".to_string());
    }
    if request
        .constraints
        .max_parallel_agents
        .is_some_and(|count| count > 4)
    {
        status = "rejected".to_string();
        policy_gates.push("max_parallel_agents_exceeded".to_string());
    }
    if matches!(request.action, RuntimeOrchestrationAction::RequestTeam)
        && request.session_id.as_deref().is_none_or(str::is_empty)
    {
        status = "rejected".to_string();
        policy_gates.push("missing_session_id_for_team_runtime".to_string());
    }
    if request.intent.trim().is_empty()
        && matches!(
            request.action,
            RuntimeOrchestrationAction::RequestSubagent
                | RuntimeOrchestrationAction::RequestVerification
                | RuntimeOrchestrationAction::RequestBackgroundReview
                | RuntimeOrchestrationAction::RequestSessionLink
        )
    {
        status = "rejected".to_string();
        policy_gates.push("empty_intent_rejected".to_string());
    }
    if matches!(
        request.action,
        RuntimeOrchestrationAction::RequestSessionLink
    ) && request.session_id.as_deref().is_none_or(str::is_empty)
    {
        status = "rejected".to_string();
        policy_gates.push("missing_session_id_for_session_link".to_string());
    }
    RuntimeOrchestrationDecision {
        selected_mode: *recommended_mode,
        selected_template: request.template_hint.clone(),
        reason: request.reason.clone().unwrap_or_else(|| {
            "runtime accepted model intent after policy and budget validation".to_string()
        }),
        policy_gates,
        budget: json!({
            "max_parallel_agents": request.constraints.max_parallel_agents.unwrap_or(2),
            "plan_only": matches!(request.action, RuntimeOrchestrationAction::PlanOnly)
        }),
        permission: json!({
            "requires_write": request.constraints.requires_write.unwrap_or(false),
            "risk": request.constraints.risk.clone().unwrap_or_else(|| "low".to_string())
        }),
        status,
    }
}
