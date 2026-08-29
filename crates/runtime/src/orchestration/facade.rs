//! Stable semantic-orchestration request and collaboration-patch facade.

use super::*;

#[must_use]
pub fn handle_runtime_orchestration_request(
    request: RuntimeOrchestrationCommand,
) -> RuntimeOrchestrationResult {
    handle_runtime_orchestration_request_with_decision(request, None)
}

#[must_use]
pub fn handle_runtime_orchestration_request_with_decision(
    mut request: RuntimeOrchestrationCommand,
    leased_decision: Option<&RuntimeExecutionDecision>,
) -> RuntimeOrchestrationResult {
    bind_strategy(&mut request, leased_decision, None);
    let plan = planner::plan_runtime_orchestration_with_decision(&request, leased_decision);
    let decision = validator::validate_request(
        &request,
        &plan.execution_decision,
        plan.model_proposal.as_ref(),
        None,
    );
    result_without_runtime(&request, decision)
}

pub async fn submit_runtime_orchestration_request(
    request: RuntimeOrchestrationCommand,
    leased_decision: Option<&RuntimeExecutionDecision>,
    services: &RuntimeServices,
    parent_execution: Option<ExecutionParentBinding>,
) -> RuntimeOrchestrationResult {
    submit_runtime_orchestration_request_controlled(
        request,
        leased_decision,
        services,
        parent_execution,
        None,
    )
    .await
}

pub(crate) async fn submit_runtime_orchestration_request_controlled(
    request: RuntimeOrchestrationCommand,
    leased_decision: Option<&RuntimeExecutionDecision>,
    services: &RuntimeServices,
    parent_execution: Option<ExecutionParentBinding>,
    cancellation: Option<crate::CancellationToken>,
) -> RuntimeOrchestrationResult {
    submit_runtime_orchestration_request_with_mode(
        request,
        leased_decision,
        services,
        parent_execution,
        cancellation,
        OrchestrationSubmissionMode::Wait,
    )
    .await
}

/// Apply a typed live-Program patch through the bounded Coordinator submit
/// path. Callers cannot provide physical graph nodes, executors or mutable
/// Team identities; supported operations are compiled from the exact durable
/// Program revision they name.
pub async fn submit_collaboration_intent_patch(
    graph_id: &str,
    patch: &harness_contract::execution_graph::CollaborationIntentPatch,
    services: &RuntimeServices,
) -> Result<RuntimeOrchestrationResult, String> {
    let graph = services
        .graph_state_store()
        .load_async(graph_id)
        .await
        .map_err(|error| format!("patch_target_load_failed:{error}"))?;
    if matches!(
        &patch.operation,
        harness_contract::execution_graph::CollaborationIntentPatchOperation::ChangeEdge { .. }
            | harness_contract::execution_graph::CollaborationIntentPatchOperation::SplitWorkstream { .. }
            | harness_contract::execution_graph::CollaborationIntentPatchOperation::MergeWorkstream { .. }
            | harness_contract::execution_graph::CollaborationIntentPatchOperation::RetireTeam { .. }
            | harness_contract::execution_graph::CollaborationIntentPatchOperation::NarrowObjective { .. }
            | harness_contract::execution_graph::CollaborationIntentPatchOperation::ExpandObjective { .. }
            | harness_contract::execution_graph::CollaborationIntentPatchOperation::SetParallelismHint { .. }
            | harness_contract::execution_graph::CollaborationIntentPatchOperation::Reprioritize { .. }
    ) {
        return Err("collaboration_patch_operation_requires_attested_source_attempt".to_string());
    }
    let request = collaboration_coordinator::compile_collaboration_intent_patch(&graph, patch)?;
    Ok(submit_runtime_orchestration_request_controlled(
        request,
        None,
        services,
        graph.parent_execution,
        None,
    )
    .await)
}

/// Apply a non-additive live Program patch only after Runtime has derived the
/// source attempt from the managed Agent binding.  The patch's own attempt
/// string is evidence, never authentication: callers that only possess JSON
/// must use the additive path or receive a typed rejection.
pub async fn submit_attested_collaboration_intent_patch(
    graph_id: &str,
    expected_source_attempt: &str,
    patch: &harness_contract::execution_graph::CollaborationIntentPatch,
    services: &RuntimeServices,
) -> Result<RuntimeOrchestrationResult, String> {
    if patch.source_attempt != expected_source_attempt {
        return Err("collaboration_patch_source_attempt_mismatch".to_string());
    }
    let graph = services
        .graph_state_store()
        .load_async(graph_id)
        .await
        .map_err(|error| format!("patch_target_load_failed:{error}"))?;
    let command = match &patch.operation {
        harness_contract::execution_graph::CollaborationIntentPatchOperation::SplitWorkstream {
            ..
        }
        | harness_contract::execution_graph::CollaborationIntentPatchOperation::MergeWorkstream {
            ..
        } => {
            let request = collaboration_coordinator::compile_collaboration_intent_patch(&graph, patch)?;
            return Ok(submit_runtime_orchestration_request_controlled(
                request,
                None,
                services,
                graph.parent_execution,
                None,
            )
            .await);
        }
        harness_contract::execution_graph::CollaborationIntentPatchOperation::ChangeEdge {
            ..
        } => ExecutionGraphCommand::ApplyCrossTeamEdgePatch {
            expected_revision: graph.revision,
            patch: Box::new(patch.clone()),
        },
        harness_contract::execution_graph::CollaborationIntentPatchOperation::RetireTeam {
            ..
        } => ExecutionGraphCommand::ApplyCollaborationTeamRetirement {
            expected_revision: graph.revision,
            patch: Box::new(patch.clone()),
        },
        harness_contract::execution_graph::CollaborationIntentPatchOperation::NarrowObjective {
            ..
        }
        | harness_contract::execution_graph::CollaborationIntentPatchOperation::ExpandObjective {
            ..
        } => ExecutionGraphCommand::ApplyCollaborationObjectiveNarrowing {
            expected_revision: graph.revision,
            patch: Box::new(patch.clone()),
        },
        harness_contract::execution_graph::CollaborationIntentPatchOperation::SetParallelismHint {
            ..
        }
        | harness_contract::execution_graph::CollaborationIntentPatchOperation::Reprioritize {
            ..
        } => ExecutionGraphCommand::ApplyCollaborationParallelismHint {
            expected_revision: graph.revision,
            patch: Box::new(patch.clone()),
        },
        _ => return submit_collaboration_intent_patch(graph_id, patch, services).await,
    };
    services
        .execution_supervisor()
        .command_graph(graph_id, command)
        .await
        .map_err(|error| format!("collaboration_patch_commit_failed:{error}"))?;
    let projection = services
        .execution_supervisor()
        .projection(graph_id)
        .await
        .map_err(|error| format!("collaboration_patch_projection_failed:{error}"))?;
    let outcome = completed_projection(
        RuntimeOrchestrationOperation::Control,
        projection,
        None,
        false,
        services,
    )?;
    let mut decision = RuntimeOrchestrationDecision {
        selected_pattern: ExecutionPattern::Collaborate,
        selected_template: None,
        reason: "applied a Runtime-attested collaboration Program patch".to_string(),
        policy_gates: Vec::new(),
        validation_findings: Vec::new(),
        adjustments: Vec::new(),
        required_approval: None,
        recovery_hints: Vec::new(),
        budget: json!({ "source_attempt": expected_source_attempt }),
        permission: json!({ "authorization": "runtime_attested_source_attempt" }),
        status: outcome.status.clone(),
    };
    if outcome.status == "blocked" {
        decision.validation_findings.push(
            "the Program patch committed, but the resulting graph is blocked by its durable completion state"
                .to_string(),
        );
    }
    Ok(result_from_outcome(
        &format!("collaboration-patch:{}", patch.canonical_digest),
        decision,
        outcome,
    ))
}

/// Compatibility facade for callers that already hold an `AddTeam` patch.
/// The generic entry point is the production owner so `RequestReview` and
/// future governed operations cannot grow a second mutation route.
pub async fn submit_collaboration_add_team_patch(
    graph_id: &str,
    patch: &harness_contract::execution_graph::CollaborationIntentPatch,
    services: &RuntimeServices,
) -> Result<RuntimeOrchestrationResult, String> {
    if !matches!(
        &patch.operation,
        harness_contract::execution_graph::CollaborationIntentPatchOperation::AddTeam { .. }
    ) {
        return Err("add_team_facade_requires_add_team_operation".to_string());
    }
    submit_collaboration_intent_patch(graph_id, patch, services).await
}

/// Admit a managed-Agent escalation through its parent Program only.  The
/// caller supplies the Runtime-attested attempt fence; an Agent-provided
/// `source_attempt` is never trusted by itself.  This intentionally has no
/// route for creating a child root graph.
pub async fn submit_collaboration_escalation(
    graph_id: &str,
    expected_source_attempt: &str,
    escalation: &harness_contract::execution_graph::CollaborationEscalationRequest,
    services: &RuntimeServices,
) -> Result<RuntimeOrchestrationResult, String> {
    escalation.validate()?;
    if escalation.source_attempt != expected_source_attempt {
        return Err("collaboration_escalation_source_attempt_mismatch".to_string());
    }
    let graph = services
        .graph_state_store()
        .load_async(graph_id)
        .await
        .map_err(|error| format!("escalation_target_load_failed:{error}"))?;
    let program_id = graph
        .orchestration
        .as_ref()
        .and_then(|metadata| metadata.collaboration_program.as_ref())
        .map(|program| program.program_id.clone())
        .ok_or_else(|| "escalation_target_has_no_collaboration_program".to_string())?;
    let mut patch = escalation.as_add_team_patch(program_id);
    if let Some(template_proposal) = escalation.template_proposal.clone() {
        attach_escalated_ephemeral_template(&graph, &mut patch, template_proposal, services)?;
    } else {
        attach_source_ephemeral_template_for_escalation(
            &graph,
            expected_source_attempt,
            &mut patch,
        )?;
    }
    submit_attested_collaboration_intent_patch(graph_id, expected_source_attempt, &patch, services)
        .await
}
