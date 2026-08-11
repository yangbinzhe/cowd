use std::collections::{BTreeMap, BTreeSet};

use harness_contract::core::ExecutionPolicyGate;
use harness_contract::execution_graph::ExecutionDependencyPolicy;
use harness_contract::policy::PermissionMode;
use harness_contract::strategy::StrategyProposal;
use serde_json::json;

use crate::execution_core::RuntimeExecutionDecision;
use crate::orchestration::request::{
    CapabilityRecipeId, GraphMutationProposal, RuntimeControlScope, RuntimeOrchestrationCommand,
    RuntimeOrchestrationOperation,
};
use crate::orchestration::result::{
    RecoveryHint, RuntimeOrchestrationApprovalRequirement, RuntimeOrchestrationDecision,
};
use crate::{ApprovalQueue, GlobalApprovalStatus};

#[must_use]
pub fn validate_request(
    request: &RuntimeOrchestrationCommand,
    execution: &RuntimeExecutionDecision,
    model_proposal: Option<&StrategyProposal>,
    approval_queue: Option<&ApprovalQueue>,
) -> RuntimeOrchestrationDecision {
    let mut policy_gates = execution.gates().to_vec();
    let mut findings = Vec::new();
    let mut status = match request.operation {
        RuntimeOrchestrationOperation::Inspect => "planned",
        RuntimeOrchestrationOperation::Propose
        | RuntimeOrchestrationOperation::Revise
        | RuntimeOrchestrationOperation::Control => "accepted",
        RuntimeOrchestrationOperation::RouteInput => "rejected",
    }
    .to_string();

    if !execution.executable && request.operation != RuntimeOrchestrationOperation::Inspect {
        reject(&mut status, &mut findings, "strategy_resources_unavailable");
        findings.extend(execution.blocked_reasons.iter().cloned());
    }
    validate_operation_shape(request, &mut status, &mut findings);
    if let Some(proposal) = request.proposal.as_ref() {
        validate_proposal(request, proposal, &mut status, &mut findings);
    }
    if model_proposal.is_some_and(|proposal| proposal.pattern != execution.pattern()) {
        reject(
            &mut status,
            &mut findings,
            "model_proposal_conflicts_with_strategy_lease",
        );
    }

    let requested_risk = request.constraints.risk.as_deref();
    if requested_risk.is_some_and(|risk| matches!(risk, "high" | "critical")) {
        push_gate(&mut policy_gates, ExecutionPolicyGate::Risk);
        findings.push("risk_gate_required".to_string());
    }
    if requested_risk == Some("critical") {
        if status != "rejected" {
            status = "needs_approval".to_string();
        }
        push_gate(&mut policy_gates, ExecutionPolicyGate::Approval);
        findings.push("risk_requires_approval".to_string());
    }
    if request.constraints.requires_write.unwrap_or(false)
        && !request
            .constraints
            .permission_ceiling
            .permits(PermissionMode::WorkspaceWrite)
    {
        reject(
            &mut status,
            &mut findings,
            "write_request_exceeds_permission_ceiling",
        );
        push_gate(&mut policy_gates, ExecutionPolicyGate::Permission);
    }
    if request
        .constraints
        .max_parallel_agents
        .is_some_and(|count| count == 0)
    {
        reject(
            &mut status,
            &mut findings,
            "max_parallel_agents_must_be_positive",
        );
    }
    if request.intent.trim().is_empty()
        && request.operation != RuntimeOrchestrationOperation::Control
    {
        reject(&mut status, &mut findings, "empty_intent_rejected");
    }

    let required_approval = approval_requirement(request, execution);
    if let Some(requirement) = required_approval.as_ref() {
        validate_global_approval(
            requirement,
            "accepted",
            &mut status,
            &mut findings,
            approval_queue,
        );
    }
    let recovery_hints = recovery_hints_for_findings(&findings);
    policy_gates.sort_by_key(|gate| gate.as_str());
    policy_gates.dedup();

    RuntimeOrchestrationDecision {
        selected_pattern: execution.pattern(),
        selected_template: request
            .proposal
            .as_ref()
            .and_then(|proposal| proposal.nodes.iter().find_map(|node| node.template.clone()))
            .or_else(|| {
                execution
                    .recommended_template
                    .map(|template| template.as_str().to_string())
            }),
        reason: request
            .proposal
            .as_ref()
            .map(|proposal| proposal.reason.clone())
            .unwrap_or_else(|| {
                format!(
                    "runtime validated semantic operation `{}` against the active strategy lease",
                    request.operation.as_str()
                )
            }),
        policy_gates,
        validation_findings: findings,
        required_approval,
        recovery_hints,
        budget: json!({
            "requested_max_parallel_agents": request.constraints.max_parallel_agents,
            "parallelism_owner": "runtime_execution_resource_manager",
            "strategy_lease_id": execution.lease.lease_id,
            "strategy_decision_id": execution.decision_id,
            "strategy_decision_revision": execution.decision_revision,
            "mutation_id": request.proposal.as_ref().map(|proposal| proposal.mutation_id.as_str()),
            "expected_graph_revision": request.proposal.as_ref().and_then(|proposal| proposal.expected_revision),
        }),
        permission: json!({
            "requires_write": request.constraints.requires_write.unwrap_or(false),
            "permission_ceiling": request.constraints.permission_ceiling,
            "risk": request.constraints.risk.clone().unwrap_or_else(|| "low".to_string())
        }),
        status,
    }
}

fn recovery_hints_for_findings(findings: &[String]) -> Vec<RecoveryHint> {
    const RULES: &[(&str, &str, &str, bool)] = &[
        (
            "Team execution requires at least one Runtime-cropped",
            "add_session_evidence_lease",
            "Runtime must derive at least one session evidence lease for in-session Team proposals",
            true,
        ),
        (
            "Team mission not found",
            "rebind_mission",
            "Re-bind the Team to the workspace default Mission or an existing mission_focus",
            true,
        ),
        (
            "proposal_exceeds_parallel_agent_ceiling",
            "reduce_parallel_width",
            "Reduce the proposed parallel width below the effective max_parallel_agents ceiling",
            true,
        ),
        (
            "model_proposal_conflicts_with_strategy_lease",
            "release_strategy_lease_or_retry",
            "Release the stale strategy lease or re-propose with per-focus partitions",
            true,
        ),
        (
            "semantic_node_resource_scope_not_leased",
            "add_resource_lease",
            "Runtime must attach a cropped resource lease before execution",
            true,
        ),
        (
            "write_request_exceeds_permission_ceiling",
            "raise_permission_ceiling",
            "The user must raise the permission ceiling before this write proposal can execute",
            false,
        ),
        (
            "absent from the Runtime catalog",
            "select_catalog_agent",
            "Select an Agent that exists in the Runtime catalog",
            true,
        ),
    ];
    let mut hints = Vec::new();
    for finding in findings {
        for (needle, code, message, retryable) in RULES {
            if finding.contains(needle)
                && !hints.iter().any(|hint: &RecoveryHint| hint.code == *code)
            {
                hints.push(RecoveryHint {
                    code: (*code).to_string(),
                    message: (*message).to_string(),
                    retryable: *retryable,
                });
            }
        }
    }
    hints
}

fn validate_operation_shape(
    request: &RuntimeOrchestrationCommand,
    status: &mut String,
    findings: &mut Vec<String>,
) {
    match request.operation {
        RuntimeOrchestrationOperation::Inspect => {
            if request.proposal.is_some()
                || request.control.is_some()
                || request.input_disposition.is_some()
            {
                reject(status, findings, "inspect_rejects_mutation_payload");
            }
        }
        RuntimeOrchestrationOperation::Propose => {
            if request.proposal.is_none()
                || request.control.is_some()
                || request.input_disposition.is_some()
            {
                reject(status, findings, "propose_requires_only_graph_proposal");
            }
            if request
                .proposal
                .as_ref()
                .is_some_and(|proposal| proposal.target_execution_id.is_some())
            {
                reject(status, findings, "propose_rejects_existing_graph_target");
            }
        }
        RuntimeOrchestrationOperation::Revise => {
            if request.proposal.as_ref().is_none_or(|proposal| {
                proposal
                    .target_execution_id
                    .as_deref()
                    .is_none_or(str::is_empty)
                    || proposal.expected_revision.is_none()
            }) || request.control.is_some()
                || request.input_disposition.is_some()
            {
                reject(
                    status,
                    findings,
                    "revise_requires_target_and_expected_revision",
                );
            }
        }
        RuntimeOrchestrationOperation::Control => {
            if request.control.is_none()
                || request.proposal.is_some()
                || request.input_disposition.is_some()
            {
                reject(status, findings, "control_requires_only_control_payload");
            }
            if let Some(control) = request.control.as_ref() {
                let scoped = matches!(
                    control.scope,
                    RuntimeControlScope::Agent
                        | RuntimeControlScope::Team
                        | RuntimeControlScope::Subgraph
                );
                if scoped && control.target_node_id.as_deref().is_none_or(str::is_empty) {
                    reject(status, findings, "scoped_control_requires_target_node");
                }
                if scoped
                    && !matches!(
                        control.action,
                        crate::orchestration::request::RuntimeControlKind::Cancel
                    )
                {
                    reject(
                        status,
                        findings,
                        "scoped_control_currently_supports_cancel_only",
                    );
                }
                if matches!(
                    control.scope,
                    RuntimeControlScope::Mission | RuntimeControlScope::Graph
                ) && control.target_node_id.is_some()
                {
                    reject(status, findings, "graph_control_rejects_target_node");
                }
            }
        }
        RuntimeOrchestrationOperation::RouteInput => {
            reject(
                status,
                findings,
                "route_input_requires_active_host_disposition_scope",
            );
        }
    }
}

fn validate_proposal(
    request: &RuntimeOrchestrationCommand,
    proposal: &GraphMutationProposal,
    status: &mut String,
    findings: &mut Vec<String>,
) {
    if proposal.mutation_id.trim().is_empty() {
        reject(status, findings, "missing_mutation_id");
    }
    if proposal.nodes.is_empty() {
        reject(status, findings, "empty_graph_mutation");
        return;
    }
    let mut ids = BTreeSet::new();
    let mut multiplicities = BTreeMap::<String, usize>::new();
    let mut indegree = BTreeMap::<String, usize>::new();
    let mut outgoing = BTreeMap::<String, Vec<String>>::new();
    let allowed_resource_scopes = request
        .capabilities
        .iter()
        .filter_map(|capability| capability.strip_prefix("resource:"))
        .collect::<Vec<_>>();
    for node in &proposal.nodes {
        if node.node_id.trim().is_empty()
            || !node.node_id.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
            })
        {
            reject(status, findings, "semantic_node_id_is_not_portable");
        }
        if !ids.insert(node.node_id.clone()) {
            reject(status, findings, "duplicate_semantic_node_id");
        }
        multiplicities.insert(node.node_id.clone(), usize::from(node.multiplicity));
        indegree.insert(node.node_id.clone(), 0);
        if node.objective.trim().is_empty() {
            reject(status, findings, "semantic_node_objective_is_empty");
        }
        if node.multiplicity == 0 || node.multiplicity > 100 {
            reject(status, findings, "semantic_node_multiplicity_out_of_range");
        }
        if node.recipe == CapabilityRecipeId::Direct {
            reject(
                status,
                findings,
                "direct_recipe_must_continue_in_the_current_turn",
            );
        }
        if !node.required
            && (matches!(
                node.recipe,
                CapabilityRecipeId::Team
                    | CapabilityRecipeId::Synthesis
                    | CapabilityRecipeId::SessionDispatch
            ) || node.resource_scopes.iter().any(|scope| {
                scope.starts_with("write:")
                    || scope.starts_with("worktree:")
                    || scope.starts_with("network:")
                    || scope.starts_with("system:")
            }))
        {
            reject(status, findings, "optional_semantic_node_owns_effect");
        }
        if node
            .cancellation_group
            .as_deref()
            .is_some_and(|group| group.trim().is_empty())
        {
            reject(status, findings, "empty_cancellation_group");
        }
        if node
            .output_artifacts
            .iter()
            .any(|artifact| artifact.trim().is_empty())
        {
            reject(status, findings, "empty_output_artifact_contract");
        }
        for scope in &node.resource_scopes {
            if !valid_relative_scope(scope)
                || !allowed_resource_scopes
                    .iter()
                    .any(|allowed| scope_within(scope, allowed))
            {
                reject(status, findings, "semantic_node_resource_scope_not_leased");
            }
        }
    }
    let concurrent_instances = maximum_parallel_instances(proposal, &multiplicities);
    if request
        .constraints
        .max_parallel_agents
        .is_some_and(|maximum| concurrent_instances > maximum)
    {
        reject(status, findings, "proposal_exceeds_parallel_agent_ceiling");
        findings.push(format!(
            "proposal_parallel_width={concurrent_instances}; requested_max_parallel_agents={}",
            request.constraints.max_parallel_agents.unwrap_or_default()
        ));
    }
    for node in &proposal.nodes {
        let predecessor_instances = node.depends_on.iter().fold(0usize, |total, dependency| {
            total.saturating_add(multiplicities.get(dependency).copied().unwrap_or_default())
        });
        match node.dependency {
            ExecutionDependencyPolicy::All => {}
            ExecutionDependencyPolicy::Any { cancel_remaining } => {
                if predecessor_instances == 0 {
                    reject(status, findings, "any_dependency_requires_predecessor");
                }
                if cancel_remaining && node.cancellation_group.is_none() {
                    reject(status, findings, "cancelling_dependency_requires_group");
                }
            }
            ExecutionDependencyPolicy::Quorum {
                minimum,
                cancel_remaining,
            } => {
                if minimum == 0 || usize::from(minimum) > predecessor_instances {
                    reject(status, findings, "quorum_dependency_is_out_of_range");
                }
                if cancel_remaining && node.cancellation_group.is_none() {
                    reject(status, findings, "cancelling_dependency_requires_group");
                }
            }
        }
        for dependency in &node.depends_on {
            if ids.contains(dependency) {
                *indegree.entry(node.node_id.clone()).or_default() += 1;
                outgoing
                    .entry(dependency.clone())
                    .or_default()
                    .push(node.node_id.clone());
            } else if request.operation == RuntimeOrchestrationOperation::Propose {
                reject(status, findings, "proposal_dependency_is_missing");
            }
        }
    }
    let mut frontier = indegree
        .iter()
        .filter_map(|(id, count)| (*count == 0).then_some(id.clone()))
        .collect::<Vec<_>>();
    let mut visited = 0usize;
    while let Some(id) = frontier.pop() {
        visited += 1;
        for target in outgoing.get(&id).into_iter().flatten() {
            let count = indegree.get_mut(target).expect("validated semantic node");
            *count -= 1;
            if *count == 0 {
                frontier.push(target.clone());
            }
        }
    }
    if visited != ids.len() {
        reject(status, findings, "proposal_dependency_cycle");
    }
}

/// Return the maximum number of semantic instances that can be runnable in
/// the same dependency wave. A ceiling is a concurrency limit, not a limit on
/// the total amount of useful work in a multi-stage graph.
fn maximum_parallel_instances(
    proposal: &GraphMutationProposal,
    multiplicities: &BTreeMap<String, usize>,
) -> usize {
    let mut levels = BTreeMap::<String, usize>::new();
    let mut unresolved = proposal.nodes.iter().collect::<Vec<_>>();
    let mut advanced = true;
    while !unresolved.is_empty() && advanced {
        advanced = false;
        unresolved.retain(|node| {
            if node
                .depends_on
                .iter()
                .all(|dependency| levels.contains_key(dependency))
            {
                let level = node
                    .depends_on
                    .iter()
                    .filter_map(|dependency| levels.get(dependency))
                    .copied()
                    .max()
                    .map_or(0, |parent| parent.saturating_add(1));
                levels.insert(node.node_id.clone(), level);
                advanced = true;
                false
            } else {
                true
            }
        });
    }
    if !unresolved.is_empty() {
        // Cycle and missing-dependency findings are emitted by the canonical
        // graph validation below. Keep this calculation conservative.
        return multiplicities.values().copied().sum();
    }
    let mut widths = BTreeMap::<usize, usize>::new();
    for node in &proposal.nodes {
        let level = levels.get(&node.node_id).copied().unwrap_or_default();
        let width = multiplicities
            .get(&node.node_id)
            .copied()
            .unwrap_or_default();
        *widths.entry(level).or_default() += width;
    }
    widths.values().copied().max().unwrap_or_default()
}

fn valid_relative_scope(scope: &str) -> bool {
    let (_, value) = scope.split_once(':').unwrap_or(("", scope));
    !value.trim().is_empty()
        && !value.starts_with('/')
        && !value.split(['/', '\\']).any(|part| part == "..")
        && !value.contains(':')
}

fn scope_within(requested: &str, allowed: &str) -> bool {
    let requested = requested
        .split_once(':')
        .map_or(requested, |(_, value)| value);
    let allowed = allowed.split_once(':').map_or(allowed, |(_, value)| value);
    requested == allowed
        || requested
            .strip_prefix(allowed)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn approval_requirement(
    request: &RuntimeOrchestrationCommand,
    execution: &RuntimeExecutionDecision,
) -> Option<RuntimeOrchestrationApprovalRequirement> {
    if request.operation == RuntimeOrchestrationOperation::Inspect
        || !execution.gates().contains(&ExecutionPolicyGate::Approval)
    {
        return None;
    }
    Some(RuntimeOrchestrationApprovalRequirement {
        action: format!("runtime_orchestrate:{}", request.operation.as_str()),
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
    accepted_status: &str,
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
    if receipt.source.session_id != requirement.session_id || receipt.action != requirement.action {
        reject(status, findings, "global_approval_binding_mismatch");
        return;
    }
    match receipt.status {
        GlobalApprovalStatus::Approved => {
            findings.push("global_approval_receipt_validated".to_string());
            if status == "needs_approval" {
                *status = accepted_status.to_string();
            }
        }
        GlobalApprovalStatus::Pending => {
            require_approval(status);
            findings.push("global_approval_pending".to_string());
        }
        GlobalApprovalStatus::Denied
        | GlobalApprovalStatus::TimedOut
        | GlobalApprovalStatus::Cancelled
        | GlobalApprovalStatus::Superseded
        | GlobalApprovalStatus::Skipped => {
            reject(status, findings, "global_approval_not_approved");
        }
    }
}

fn reject(status: &mut String, findings: &mut Vec<String>, finding: &str) {
    *status = "rejected".to_string();
    findings.push(finding.to_string());
}

fn require_approval(status: &mut String) {
    if status != "rejected" {
        *status = "needs_approval".to_string();
    }
}

fn push_gate(gates: &mut Vec<ExecutionPolicyGate>, gate: ExecutionPolicyGate) {
    if !gates.contains(&gate) {
        gates.push(gate);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parallel_ceiling_finding_gets_retryable_hint() {
        let hints =
            recovery_hints_for_findings(&["proposal_exceeds_parallel_agent_ceiling".to_string()]);
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].code, "reduce_parallel_width");
        assert!(hints[0].retryable);
    }

    #[test]
    fn team_evidence_lease_finding_gets_session_hint() {
        let hints = recovery_hints_for_findings(&[
            "semantic_compile_failed:Team template resolution failed: invalid contract: Team execution requires at least one Runtime-cropped filesystem, network, or session evidence lease"
                .to_string(),
        ]);
        assert!(hints
            .iter()
            .any(|hint| hint.code == "add_session_evidence_lease"));
    }
}
