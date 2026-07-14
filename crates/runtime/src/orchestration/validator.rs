use harness_contract::core::{ExecutionPattern, ExecutionPolicyGate};
use serde_json::json;

use crate::execution_core::RuntimeExecutionDecision;
use crate::orchestration::request::{RuntimeOrchestrationAction, RuntimeOrchestrationRequest};
use crate::orchestration::result::{
    RuntimeOrchestrationApprovalRequirement, RuntimeOrchestrationDecision,
};
use crate::{ApprovalQueue, GlobalApprovalStatus};
use harness_contract::strategy::StrategyProposal;

#[must_use]
pub fn validate_request(
    request: &RuntimeOrchestrationRequest,
    execution: &RuntimeExecutionDecision,
    model_proposal: Option<&StrategyProposal>,
    approval_queue: Option<&ApprovalQueue>,
) -> RuntimeOrchestrationDecision {
    let mut policy_gates = execution.gates().to_vec();
    let mut findings = Vec::new();
    let dispatch_status = match request.action {
        RuntimeOrchestrationAction::RequestTeam
        | RuntimeOrchestrationAction::RequestSubagent
        | RuntimeOrchestrationAction::RequestVerification
        | RuntimeOrchestrationAction::RequestBackgroundReview
        | RuntimeOrchestrationAction::DispatchSession => "accepted",
        RuntimeOrchestrationAction::RequestRiskGate => "needs_approval",
        _ => "planned",
    };
    let mut status = dispatch_status.to_string();

    if !execution.executable {
        status = "rejected".to_string();
        findings.push("strategy_resources_unavailable".to_string());
        findings.extend(execution.blocked_reasons.iter().cloned());
    }
    if !strategy_authorizes(request.action, execution) {
        status = "rejected".to_string();
        findings.push("action_not_authorized_by_strategy".to_string());
    }
    if model_proposal.is_some_and(|proposal| proposal.pattern != execution.pattern()) {
        status = "rejected".to_string();
        findings.push("model_proposal_conflicts_with_strategy_lease".to_string());
    }
    if request.action == RuntimeOrchestrationAction::RequestRiskGate {
        push_gate(&mut policy_gates, ExecutionPolicyGate::Risk);
        push_gate(&mut policy_gates, ExecutionPolicyGate::Approval);
        findings.push("risk_gate_requested".to_string());
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
        push_gate(&mut policy_gates, ExecutionPolicyGate::Risk);
        push_gate(&mut policy_gates, ExecutionPolicyGate::Approval);
        findings.push("risk_requires_approval".to_string());
    }
    if request.constraints.requires_write.unwrap_or(false)
        && !execution.gates().contains(&ExecutionPolicyGate::Permission)
    {
        status = "rejected".to_string();
        push_gate(&mut policy_gates, ExecutionPolicyGate::Permission);
        findings.push("write_scope_not_present_in_strategy_lease".to_string());
    }
    if request
        .constraints
        .max_parallel_agents
        .is_some_and(|count| count == 0)
    {
        status = "rejected".to_string();
        findings.push("max_parallel_agents_must_be_positive".to_string());
    }
    if request.action == RuntimeOrchestrationAction::RequestTeam
        && request.session_id.as_deref().is_none_or(str::is_empty)
    {
        status = "rejected".to_string();
        findings.push("missing_session_id_for_team_runtime".to_string());
    }
    if request.intent.trim().is_empty()
        && !matches!(request.action, RuntimeOrchestrationAction::PlanOnly)
    {
        status = "rejected".to_string();
        findings.push("empty_intent_rejected".to_string());
    }
    if request.action == RuntimeOrchestrationAction::DispatchSession
        && request.session_id.as_deref().is_none_or(str::is_empty)
    {
        status = "rejected".to_string();
        findings.push("missing_source_session_id_for_dispatch".to_string());
    }
    if request.action == RuntimeOrchestrationAction::DispatchSession
        && request
            .target_session_id
            .as_deref()
            .is_none_or(str::is_empty)
    {
        status = "rejected".to_string();
        findings.push("missing_target_session_id_for_dispatch".to_string());
    }
    let required_approval = approval_requirement(request, execution);
    if let Some(requirement) = required_approval.as_ref() {
        validate_global_approval(
            requirement,
            dispatch_status,
            &mut status,
            &mut findings,
            approval_queue,
        );
    }
    policy_gates.sort_by_key(|gate| gate.as_str());
    policy_gates.dedup();

    RuntimeOrchestrationDecision {
        selected_pattern: execution.pattern(),
        selected_template: request.template_hint.clone().or_else(|| {
            execution
                .recommended_template
                .map(|template| template.as_str().to_string())
        }),
        reason: request.reason.clone().unwrap_or_else(|| {
            "runtime compiled model intent through the leased strategy decision".to_string()
        }),
        policy_gates,
        validation_findings: findings,
        required_approval,
        budget: json!({
            "requested_max_parallel_agents": request.constraints.max_parallel_agents,
            "parallelism_owner": "runtime_team_instantiation_resource_policy",
            "plan_only": request.action == RuntimeOrchestrationAction::PlanOnly,
            "strategy_lease_id": execution.lease.lease_id,
        }),
        permission: json!({
            "requires_write": request.constraints.requires_write.unwrap_or(false),
            "risk": request.constraints.risk.clone().unwrap_or_else(|| "low".to_string())
        }),
        status,
    }
}

fn approval_requirement(
    request: &RuntimeOrchestrationRequest,
    execution: &RuntimeExecutionDecision,
) -> Option<RuntimeOrchestrationApprovalRequirement> {
    if !execution.gates().contains(&ExecutionPolicyGate::Approval)
        || matches!(
            request.action,
            RuntimeOrchestrationAction::PlanOnly | RuntimeOrchestrationAction::RequestRiskGate
        )
    {
        return None;
    }

    Some(RuntimeOrchestrationApprovalRequirement {
        action: format!("runtime_orchestrate:{}", request.action.as_str()),
        session_id: request.session_id.clone(),
        approval_id: request
            .constraints
            .approval_id
            .as_deref()
            .map(str::trim)
            .filter(|approval_id| !approval_id.is_empty())
            .map(str::to_string),
    })
}

fn validate_global_approval(
    requirement: &RuntimeOrchestrationApprovalRequirement,
    dispatch_status: &str,
    status: &mut String,
    findings: &mut Vec<String>,
    approval_queue: Option<&ApprovalQueue>,
) {
    let Some(approval_id) = requirement.approval_id.as_deref() else {
        require_approval(status);
        findings.push("missing_global_approval_receipt".to_string());
        return;
    };
    let Some(receipt) = approval_queue.and_then(|queue| queue.get(approval_id)) else {
        require_approval(status);
        findings.push("global_approval_receipt_not_found".to_string());
        return;
    };

    let mut mismatched = false;
    if receipt.source.session_id != requirement.session_id {
        findings.push("global_approval_session_mismatch".to_string());
        mismatched = true;
    }
    if receipt.action != requirement.action {
        findings.push("global_approval_action_mismatch".to_string());
        mismatched = true;
    }
    if mismatched {
        *status = "rejected".to_string();
        return;
    }

    match receipt.status {
        GlobalApprovalStatus::Approved => {
            findings.push("global_approval_receipt_validated".to_string());
            if status == "needs_approval" {
                *status = dispatch_status.to_string();
            }
        }
        GlobalApprovalStatus::Pending => {
            require_approval(status);
            findings.push("global_approval_pending".to_string());
        }
        GlobalApprovalStatus::Denied => {
            *status = "rejected".to_string();
            findings.push("global_approval_denied".to_string());
        }
        GlobalApprovalStatus::TimedOut => {
            *status = "rejected".to_string();
            findings.push("global_approval_timed_out".to_string());
        }
    }
}

fn require_approval(status: &mut String) {
    if status != "rejected" {
        *status = "needs_approval".to_string();
    }
}

fn strategy_authorizes(
    action: RuntimeOrchestrationAction,
    execution: &RuntimeExecutionDecision,
) -> bool {
    use RuntimeOrchestrationAction as Action;

    match action {
        Action::PlanOnly | Action::RequestRiskGate => true,
        Action::RequestParallelTools => {
            execution.pattern() == ExecutionPattern::Explore
                && execution
                    .modifiers()
                    .contains(&harness_contract::core::ExecutionModifier::Parallel)
        }
        Action::RequestRewooEvidence => execution.pattern() == ExecutionPattern::Explore,
        Action::RequestSubagent => execution.pattern() == ExecutionPattern::Execute,
        Action::RequestVerification | Action::RequestReflexionRetry => {
            execution.pattern() != ExecutionPattern::Direct
        }
        Action::RequestDeliberation => execution.pattern() == ExecutionPattern::Deliberate,
        Action::RequestTeam => execution.pattern() == ExecutionPattern::Collaborate,
        Action::RequestBackgroundReview | Action::DispatchSession => {
            execution.pattern() == ExecutionPattern::Supervise
        }
    }
}

fn push_gate(gates: &mut Vec<ExecutionPolicyGate>, gate: ExecutionPolicyGate) {
    if !gates.contains(&gate) {
        gates.push(gate);
    }
}
