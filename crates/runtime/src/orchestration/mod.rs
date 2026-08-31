//! Model-visible semantic orchestration over Runtime-owned execution graphs.
//!
//! The model may inspect state and propose semantic topology. Runtime alone
//! resolves definitions, executors, leases, physical identities and commands.

pub mod collaboration_continuation;
pub(crate) mod collaboration_coordinator;
pub mod compiler;
pub(crate) mod input_disposition;
pub mod intent_compiler;
pub mod planner;
pub mod request;
pub mod result;
pub(crate) mod team_authority;
pub mod validator;

mod facade;
pub use facade::{
    handle_runtime_orchestration_request, handle_runtime_orchestration_request_with_decision,
    submit_attested_collaboration_intent_patch, submit_collaboration_add_team_patch,
    submit_collaboration_escalation, submit_collaboration_intent_patch,
    submit_runtime_orchestration_request,
};

use std::collections::{BTreeMap, BTreeSet};

use crate::execution_core::graph::{ExecutionCommitError, ExecutionRunnerError};
use crate::execution_core::{
    graph::ExecutionRunReport, ExecutionStateStoreError, RuntimeExecutionDecision,
};
use crate::{
    ApprovalSource, ApprovalSourceKind, ApprovalTimeoutPolicy, ExecutionGraphHost, RuntimeServices,
    SubmitGlobalApprovalRequest,
};
use harness_contract::core::{ExecutionPattern, TaskRisk};
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphCommand, ExecutionGraphProjection, ExecutionNodeKind,
    ExecutionNodeStatus, ExecutionParentBinding, ExecutionUsage,
};
use harness_contract::policy::{
    ApprovalContext, ApprovalDecisionActor, ApprovalDecisionActorKind, ApprovalDecisionCommand,
    ApprovalDomain, ApprovalGrantScope,
};
use serde_json::{json, Value};

const MAX_REVISION_CAS_ATTEMPTS: usize = 3;
const EPHEMERAL_TEMPLATE_TTL_MS: u64 = 60 * 60 * 1000;

/// A revision of an admitted Program must reuse the capacity facts captured
/// by its first admission. Hot-reloaded process policy may govern later
/// Programs, never an already durable Program revision.
fn capacity_snapshot_from_existing_program(
    graph: &ExecutionGraph,
) -> Result<harness_contract::team::TeamExecutionCapacitySnapshot, String> {
    let mut snapshot = None;
    for node in &graph.nodes {
        if node.kind != ExecutionNodeKind::Subgraph {
            continue;
        }
        let Ok(request) = serde_json::from_str::<harness_contract::team::TeamInstantiationRequest>(
            &node.payload_ref,
        ) else {
            continue;
        };
        let current = request
            .execution_capacity
            .ok_or_else(|| format!("program_team_capacity_snapshot_missing:{}", node.id))?;
        if let Some(existing) = snapshot.as_ref() {
            if existing != &current {
                return Err("program_team_capacity_snapshot_mismatch".to_string());
            }
        } else {
            snapshot = Some(current);
        }
    }
    snapshot.ok_or_else(|| "program_capacity_snapshot_missing".to_string())
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;

pub use compiler::CompiledOrchestration;
pub use planner::RuntimeOrchestrationPlan;
pub use request::{
    CapabilityRecipeId, GraphMutationProposal, GraphSemanticNode, RuntimeControlKind,
    RuntimeControlRequest, RuntimeControlScope, RuntimeOrchestrationBinding,
    RuntimeOrchestrationCommand, RuntimeOrchestrationConstraints, RuntimeOrchestrationOperation,
    SemanticFocus,
};
pub use result::{
    RecoveryHint, RuntimeOrchestrationApprovalRequirement, RuntimeOrchestrationDecision,
    RuntimeOrchestrationResult, RuntimeStateSnapshot,
};

/// An Agent-triggered follow-up is an additive Program revision, not a new
/// model template-selection opportunity.  When its source Team was a
/// turn-scoped custom Team, clone that already-frozen snapshot onto the
/// follow-up patch.  The older implementation attempted to recover a catalog
/// selector from an arbitrary root seed and rejected every such escalation
/// with `patch_target_seed_has_ephemeral_template_selector`.
fn attach_source_ephemeral_template_for_escalation(
    graph: &ExecutionGraph,
    expected_source_attempt: &str,
    patch: &mut harness_contract::execution_graph::CollaborationIntentPatch,
) -> Result<(), String> {
    let harness_contract::execution_graph::CollaborationIntentPatchOperation::AddTeam { team } =
        &mut patch.operation
    else {
        return Err("escalation_source_template_requires_add_team_operation".to_string());
    };
    if team.ephemeral_template.is_some() {
        return Err("escalation_source_template_snapshot_must_be_runtime_owned".to_string());
    }
    let source_seed = graph
        .nodes
        .iter()
        .filter_map(|node| {
            serde_json::from_str::<harness_contract::team::TeamInstantiationRequest>(
                &node.payload_ref,
            )
            .ok()
        })
        .filter(|request| {
            expected_source_attempt.starts_with(&format!("team-graph:{}:", request.team_id))
        })
        .collect::<Vec<_>>();
    let [source_seed] = source_seed.as_slice() else {
        return Err("escalation_source_attempt_has_no_unique_parent_team_seed".to_string());
    };
    if !expected_source_attempt
        .strip_prefix(&format!("team-graph:{}:", source_seed.team_id))
        .is_some_and(|suffix| suffix.rsplit_once(":attempt:").is_some())
    {
        return Err("escalation_source_attempt_is_not_a_fenced_team_agent_attempt".to_string());
    }
    if let harness_contract::team::TeamTemplateSelector::Ephemeral { snapshot } =
        &source_seed.template_selector
    {
        team.ephemeral_template = Some((**snapshot).clone());
    }
    Ok(())
}

fn attach_escalated_ephemeral_template(
    graph: &ExecutionGraph,
    patch: &mut harness_contract::execution_graph::CollaborationIntentPatch,
    template_proposal: serde_json::Value,
    services: &RuntimeServices,
) -> Result<(), String> {
    let harness_contract::execution_graph::CollaborationIntentPatchOperation::AddTeam { team } =
        &mut patch.operation
    else {
        return Err("escalation_template_requires_add_team_operation".to_string());
    };
    if team.ephemeral_template.is_some() {
        return Err("escalation_template_snapshot_must_be_runtime_owned".to_string());
    }
    let seed = graph
        .nodes
        .iter()
        .find_map(|node| {
            serde_json::from_str::<harness_contract::team::TeamInstantiationRequest>(
                &node.payload_ref,
            )
            .ok()
        })
        .ok_or_else(|| "escalation_template_parent_has_no_team_lineage_seed".to_string())?;
    let policy_ref = services
        .session_execution_policy(&seed.lineage.session_id)
        .map(|policy| {
            format!(
                "session:{}:policy:{}",
                seed.lineage.session_id, policy.revision
            )
        })
        .unwrap_or_else(|| format!("session:{}:unversioned", seed.lineage.session_id));
    team.ephemeral_template = Some(compile_ephemeral_team_template_snapshot(
        template_proposal,
        &seed.lineage,
        seed.permission_ceiling,
        policy_ref,
        crate::tool_invocation::now_ms().saturating_add(EPHEMERAL_TEMPLATE_TTL_MS),
        services,
    )?);
    Ok(())
}

/// Produce a session/turn-bound custom Team snapshot without publishing a
/// reusable catalog revision. Callers must attach the returned snapshot to a
/// Coordinator-owned semantic Team request; it is not itself executable.
pub fn compile_ephemeral_team_template_snapshot(
    proposal_value: serde_json::Value,
    lineage: &harness_contract::execution_graph::ExecutionGraphLineage,
    permission_ceiling: harness_contract::policy::PermissionMode,
    policy_ref: String,
    expires_at_ms: u64,
    services: &RuntimeServices,
) -> Result<harness_contract::execution_graph::EphemeralTeamTemplateSnapshot, String> {
    collaboration_coordinator::compile_ephemeral_team_template_snapshot(
        proposal_value,
        lineage,
        permission_ceiling,
        policy_ref,
        expires_at_ms,
        services,
    )
}

pub(crate) async fn admit_runtime_orchestration_request_background(
    request: RuntimeOrchestrationCommand,
    leased_decision: Option<&RuntimeExecutionDecision>,
    services: &RuntimeServices,
    parent_execution: Option<ExecutionParentBinding>,
) -> RuntimeOrchestrationResult {
    submit_runtime_orchestration_request_with_mode(
        request,
        leased_decision,
        services,
        parent_execution,
        None,
        OrchestrationSubmissionMode::AdmitBackground,
    )
    .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OrchestrationSubmissionMode {
    Wait,
    AdmitBackground,
}

async fn submit_runtime_orchestration_request_with_mode(
    mut request: RuntimeOrchestrationCommand,
    leased_decision: Option<&RuntimeExecutionDecision>,
    services: &RuntimeServices,
    parent_execution: Option<ExecutionParentBinding>,
    cancellation: Option<crate::CancellationToken>,
    submission_mode: OrchestrationSubmissionMode,
) -> RuntimeOrchestrationResult {
    if let Some(intent) = request.collaboration_intent.take() {
        match intent_compiler::compile_turn_scoped_intent(&request, &intent, services) {
            Ok(compiled) => {
                request.proposal = Some(compiled.proposal);
                request.template_proposal = Some(compiled.template_proposal);
                request.collaboration_semantic_intent = Some(compiled.semantic_intent);
                // v2 semantic intent always describes the complete active
                // role set; the compiler, not a later model-assisted focus
                // selector, owns the exact turn-scoped Team snapshot.
                request.selection_mode = Some(harness_contract::team::TeamSelectionMode::Explicit);
            }
            Err(error) => return rejected_intent_compiler_result(&request, error),
        }
    }
    bind_strategy(&mut request, leased_decision, parent_execution.as_ref());
    if let Err(error) = validate_collaboration_obligation_cardinality(&request, leased_decision) {
        return rejected_explicit_team_cardinality_result(&request, error);
    }
    if let Err(error) = materialize_ephemeral_team_template(&mut request, services) {
        return rejected_ephemeral_template_result(&request, error);
    }
    let understanding = leased_decision
        .map(|decision| decision.strategy.understanding.clone())
        .unwrap_or_else(|| planner::understand_runtime_orchestration_request(&request));
    team_authority::bind_semantic_resource_authority_with_understanding(
        &mut request,
        &understanding,
        services.workspace_root(),
    );
    tracing::debug!(
        session = ?request.session_id,
        ceiling = ?request.constraints.permission_ceiling,
        requires_write = ?request.constraints.requires_write,
        nodes = ?request.proposal.as_ref().map(|proposal| {
            proposal
                .nodes
                .iter()
                .map(|node| {
                    (
                        node.node_id.as_str(),
                        format!("{:?}", node.recipe),
                        node.template.as_deref().unwrap_or(""),
                        node.output_artifacts.as_slice(),
                        node.resource_scopes.as_slice(),
                    )
                })
                .collect::<Vec<_>>()
        }),
        "orchestration bound semantic authority"
    );
    // Shared-context occupancy prediction for strategy/projection (toggle via
    // RUST_LOG=runtime::orchestration=debug). Display-only; never admits or
    // rejects execution.
    if let Some(proposal) = request.proposal.as_ref() {
        let window = u64::from(provider::model_context_window(
            request.model_lease.as_deref().unwrap_or("unknown"),
        ));
        for node in proposal
            .nodes
            .iter()
            .filter(|node| node.recipe == CapabilityRecipeId::Team)
        {
            let chars = node.objective.chars().count().saturating_add(
                node.resource_scopes
                    .iter()
                    .map(|scope| scope.len())
                    .sum::<usize>(),
            );
            let estimate = crate::context_occupancy::estimate_role_occupancy(
                &node.node_id,
                chars,
                0,
                0,
                window,
            );
            tracing::debug!(
                node = %node.node_id,
                occupancy_bp = estimate.utilization_bp,
                "predicted team node context occupancy"
            );
        }
    }
    let requires_orchestration_approval = request.constraints.risk.as_deref() == Some("critical");
    if requires_orchestration_approval && request.constraints.approval_id.is_none() {
        if let Err(error) = submit_approval(&mut request, services).await {
            return unavailable_result(&request, format!("approval_submission_failed:{error}"));
        }
    }
    let plan = planner::plan_runtime_orchestration_with_understanding(
        &request,
        leased_decision,
        crate::execution_core::StrategyResourceHealth {
            provider_available: true,
            tools_available: true,
            collaboration_available: true,
            mission_available: true,
            observed: true,
        },
        understanding,
    );
    let mut decision = validator::validate_request(
        &request,
        &plan.execution_decision,
        plan.model_proposal.as_ref(),
        Some(services.approval_queue().as_ref()),
    );
    if matches!(decision.status.as_str(), "rejected" | "needs_approval") {
        // Explicit multi-Team proposals are the primary parallelism contract.
        // When the model-requested ceiling is narrower than its own proposal,
        // Runtime repairs the inconsistency up to the proposal width instead
        // of hard-rejecting the turn. The safety ceiling is still enforced by
        // the resource manager at admission time.
        if decision
            .validation_findings
            .iter()
            .any(|finding| finding == "proposal_exceeds_parallel_agent_ceiling")
        {
            let width = request.proposal.as_ref().map_or(0, |proposal| {
                proposal
                    .nodes
                    .iter()
                    .map(|node| usize::from(node.multiplicity))
                    .sum()
            });
            if width > 0
                && request
                    .constraints
                    .max_parallel_agents
                    .is_some_and(|maximum| width > maximum)
            {
                request.constraints.max_parallel_agents = Some(width);
                let repaired = validator::validate_request(
                    &request,
                    &plan.execution_decision,
                    plan.model_proposal.as_ref(),
                    Some(services.approval_queue().as_ref()),
                );
                if !repaired
                    .validation_findings
                    .iter()
                    .any(|finding| finding == "proposal_exceeds_parallel_agent_ceiling")
                {
                    decision = repaired;
                    // P14-F4: successful repair is an adjustment, not a
                    // rejection finding.
                    decision
                        .adjustments
                        .push("parallel_ceiling_elevated_for_explicit_team".to_string());
                }
            }
        }
    }
    if matches!(decision.status.as_str(), "rejected" | "needs_approval") {
        return result_without_runtime(&request, decision);
    }

    let request_id = request_id(&request);
    let operation = request.operation;
    let outcome = match operation {
        RuntimeOrchestrationOperation::Inspect => inspect(&request, services).await,
        RuntimeOrchestrationOperation::Propose => {
            propose(
                &request_id,
                &request,
                &plan,
                services,
                parent_execution,
                cancellation,
                submission_mode,
            )
            .await
        }
        RuntimeOrchestrationOperation::ProposeTemplate => {
            propose_template(&request_id, &request, services).await
        }
        RuntimeOrchestrationOperation::Revise => {
            revise(&request_id, &request, &plan, services, cancellation).await
        }
        RuntimeOrchestrationOperation::Control => control(&request, services).await,
        RuntimeOrchestrationOperation::RouteInput => {
            Err(
                "route_input is not supported by runtime_orchestrate; available operations: inspect, propose, revise, control"
                    .to_string(),
            )
        }
    };
    match outcome {
        Ok(outcome) => {
            decision.status = outcome.status.clone();
            result_from_outcome(&request_id, decision, outcome)
        }
        Err(error) => {
            decision.status = "blocked".to_string();
            decision.validation_findings.push(error);
            if decision.recovery_hints.is_empty() {
                decision.recovery_hints = vec![RecoveryHint {
                    code: "blocked_recovery".to_string(),
                    message: "The orchestration proposal was blocked; review the findings and retry with a repaired proposal"
                        .to_string(),
                    retryable: true,
                }];
            }
            result_without_runtime_with_id(&request_id, &request, decision)
        }
    }
}

fn rejected_intent_compiler_result(
    request: &RuntimeOrchestrationCommand,
    error: intent_compiler::IntentCompilerError,
) -> RuntimeOrchestrationResult {
    let recovery_hints = match &error {
        intent_compiler::IntentCompilerError::Diagnostic(diagnostic) => vec![RecoveryHint {
            code: format!("collaboration_compile_{}", diagnostic.code),
            message: format!(
                "Repair only {:?}; allowed repair: {}.",
                diagnostic.field_paths,
                diagnostic.allowed_repairs.join(", ")
            ),
            retryable: diagnostic.repairability == "model_revise",
        }],
        intent_compiler::IntentCompilerError::Internal(_) => vec![RecoveryHint {
            code: "collaboration_compile_internal".to_string(),
            message: "Runtime could not compile the semantic decision; retain the submission evidence and retry only after Runtime recovery.".to_string(),
            retryable: false,
        }],
    };
    // A semantic submission intentionally reaches this point before a graph
    // proposal exists. Running the generic Propose validator here used to add
    // `propose_requires_only_graph_proposal`, a false primary diagnosis that
    // instructed the model to use the very graph payload this narrow port
    // forbids. The compiler diagnostic is the sole authoritative rejection.
    let decision = RuntimeOrchestrationDecision {
        selected_pattern: ExecutionPattern::Collaborate,
        selected_template: None,
        reason: "Runtime rejected the semantic collaboration decision before graph lowering"
            .to_string(),
        policy_gates: Vec::new(),
        validation_findings: vec![format!("collaboration_compile_diagnostic:{error}")],
        adjustments: Vec::new(),
        required_approval: None,
        recovery_hints,
        budget: json!({
            "requested_max_parallel_agents": request.constraints.max_parallel_agents,
            "parallelism_owner": "runtime_execution_resource_manager",
        }),
        permission: json!({
            "requires_write": request.constraints.requires_write.unwrap_or(false),
            "permission_ceiling": request.constraints.permission_ceiling,
            "risk": request.constraints.risk.clone().unwrap_or_else(|| "low".to_string()),
        }),
        status: "rejected".to_string(),
    };
    result_without_runtime(request, decision)
}

/// A user-declared Team cardinality is an admission obligation, not a soft
/// planner hint.  The root transport may choose the semantic workstreams, but
/// it may neither collapse required Teams nor add hidden pre-planned Teams.
/// This is intentionally evaluated against the already-bound turn strategy,
/// never against a model-supplied count.
fn validate_collaboration_obligation_cardinality(
    request: &RuntimeOrchestrationCommand,
    leased_decision: Option<&RuntimeExecutionDecision>,
) -> Result<(), String> {
    let Some(obligation) =
        leased_decision.and_then(|decision| decision.collaboration_obligation.as_ref())
    else {
        return Ok(());
    };
    if request.operation != RuntimeOrchestrationOperation::Propose {
        return Ok(());
    }
    let required = u16::from(obligation.minimum_team_count);
    let actual = request.proposal.as_ref().map_or(0, |proposal| {
        proposal
            .nodes
            .iter()
            .filter(|node| node.recipe == CapabilityRecipeId::Team)
            .map(|node| node.multiplicity)
            .sum::<u16>()
    });
    let cardinality_valid = obligation
        .exact_team_count
        .map_or(actual >= required, |exact| actual == u16::from(exact));
    cardinality_valid.then_some(()).ok_or_else(|| {
        format!(
            "collaboration_obligation_count_mismatch:source={:?}:minimum={required}:exact={:?}:proposed={actual}",
            obligation.source, obligation.exact_team_count
        )
    })
}

fn rejected_explicit_team_cardinality_result(
    request: &RuntimeOrchestrationCommand,
    finding: String,
) -> RuntimeOrchestrationResult {
    let plan = planner::plan_runtime_orchestration(request);
    let mut decision = validator::validate_request(
        request,
        &plan.execution_decision,
        plan.model_proposal.as_ref(),
        None,
    );
    decision.status = "rejected".to_string();
    decision.validation_findings.push(finding);
    result_without_runtime(request, decision)
}

/// Materialize model-authored custom Teams. Model JSON provides only semantic
/// topology and template content; Runtime alone creates immutable snapshots
/// and binds them to the authenticated session/turn lineage. A single Team
/// continues to use the direct `template_proposal` object. Multiple Teams use
/// `{ "teams": [{ "node_id": "…", "template": { … } }] }`; the node id is
/// an admission-time binding, never a catalog selector.
fn materialize_ephemeral_team_template(
    request: &mut RuntimeOrchestrationCommand,
    services: &RuntimeServices,
) -> Result<(), String> {
    let Some(template_proposal) = request.template_proposal.take() else {
        return Ok(());
    };
    if request.operation != RuntimeOrchestrationOperation::Propose {
        request.template_proposal = Some(template_proposal);
        return Ok(());
    }
    if !request.ephemeral_team_templates.is_empty() {
        return Err("ephemeral_template_already_materialized".to_string());
    }
    let lineage = request
        .lineage
        .as_ref()
        .ok_or_else(|| "ephemeral_template_requires_bound_lineage".to_string())?;
    let proposal = request
        .proposal
        .as_ref()
        .ok_or_else(|| "ephemeral_template_requires_graph_proposal".to_string())?;
    if proposal.target_execution_id.is_some() {
        return Err("ephemeral_template_rejects_existing_graph_target".to_string());
    }
    let team_nodes = proposal
        .nodes
        .iter()
        .filter(|node| node.recipe == CapabilityRecipeId::Team)
        .collect::<Vec<_>>();
    let proposals = bind_ephemeral_template_proposals(template_proposal, &team_nodes)?;
    let policy_ref = request
        .session_id
        .as_deref()
        .and_then(|session_id| {
            services
                .session_execution_policy(session_id)
                .map(|policy| format!("session:{session_id}:policy:{}", policy.revision))
        })
        .unwrap_or_else(|| format!("session:{}:unversioned", lineage.session_id));
    for (team_node, template_proposal) in proposals {
        if team_node.template.is_some() {
            return Err(format!(
                "ephemeral_template_rejects_catalog_template_selector:{}",
                team_node.node_id
            ));
        }
        let snapshot = compile_ephemeral_team_template_snapshot(
            template_proposal,
            lineage,
            request.constraints.permission_ceiling,
            policy_ref.clone(),
            crate::tool_invocation::now_ms().saturating_add(EPHEMERAL_TEMPLATE_TTL_MS),
            services,
        )?;
        request
            .ephemeral_team_templates
            .insert(team_node.node_id.clone(), snapshot);
    }
    Ok(())
}

fn bind_ephemeral_template_proposals<'a>(
    proposal: Value,
    team_nodes: &[&'a GraphSemanticNode],
) -> Result<Vec<(&'a GraphSemanticNode, Value)>, String> {
    if team_nodes.is_empty() {
        return Err("ephemeral_template_requires_at_least_one_team_node".to_string());
    }
    let Some(entries) = proposal.get("teams").and_then(Value::as_array) else {
        let [team_node] = team_nodes else {
            return Err("ephemeral_template_requires_named_team_bindings".to_string());
        };
        return Ok(vec![(team_node, proposal)]);
    };
    if entries.is_empty() {
        return Err("ephemeral_template_teams_is_empty".to_string());
    }
    if entries.len() != team_nodes.len() {
        return Err(format!(
            "ephemeral_template_team_count_mismatch:teams={}:templates={}",
            team_nodes.len(),
            entries.len()
        ));
    }
    let known = team_nodes
        .iter()
        .map(|node| (node.node_id.as_str(), *node))
        .collect::<BTreeMap<_, _>>();
    let mut bound = Vec::with_capacity(entries.len());
    let mut seen = BTreeSet::new();
    for entry in entries {
        let node_id = entry
            .get("node_id")
            .and_then(Value::as_str)
            .filter(|node_id| !node_id.trim().is_empty())
            .ok_or_else(|| "ephemeral_template_team_binding_missing_node_id".to_string())?;
        if !seen.insert(node_id) {
            return Err(format!(
                "ephemeral_template_duplicate_team_binding:{node_id}"
            ));
        }
        let team_node = known
            .get(node_id)
            .copied()
            .ok_or_else(|| format!("ephemeral_template_unknown_team_node:{node_id}"))?;
        let template = entry
            .get("template")
            .cloned()
            .ok_or_else(|| format!("ephemeral_template_binding_missing_template:{node_id}"))?;
        if !template.is_object() {
            return Err(format!(
                "ephemeral_template_binding_template_not_object:{node_id}"
            ));
        }
        bound.push((team_node, template));
    }
    Ok(bound)
}

fn rejected_ephemeral_template_result(
    request: &RuntimeOrchestrationCommand,
    finding: String,
) -> RuntimeOrchestrationResult {
    let plan = planner::plan_runtime_orchestration(request);
    let mut decision = validator::validate_request(
        request,
        &plan.execution_decision,
        plan.model_proposal.as_ref(),
        None,
    );
    decision.status = "rejected".to_string();
    decision.validation_findings.push(finding);
    result_without_runtime(request, decision)
}

async fn propose_template(
    request_id: &str,
    request: &RuntimeOrchestrationCommand,
    services: &RuntimeServices,
) -> Result<OperationOutcome, String> {
    let proposal_value = request
        .template_proposal
        .as_ref()
        .ok_or_else(|| "template_proposal_missing".to_string())?;
    let mut normalized_proposal = proposal_value.clone();
    let normalization_notes =
        crate::team_template_candidate::normalize_template_proposal(&mut normalized_proposal)
            .map_err(|error| format!("invalid_template_proposal:{error}"))?;
    let proposal: crate::team_template_candidate::TeamTemplateProposal =
        serde_json::from_value(normalized_proposal)
            .map_err(|error| format!("invalid_template_proposal:{error}"))?;
    let mut candidate = crate::team_template_candidate::TemplateCandidateCompiler::compile(
        services.definition_registry(),
        &proposal,
        request.constraints.permission_ceiling,
    )?;
    if !normalization_notes.is_empty() {
        candidate.preview["normalization_notes"] = serde_json::json!(normalization_notes);
    }
    let template_id = candidate.manifest.template_id.as_str().to_string();
    let approval_id = format!("template-approval:{}", uuid::Uuid::new_v4());
    let risk = if candidate.manifest.roles.iter().any(|role| {
        role.grant_ceiling
            .contains(&harness_contract::agent::AgentCapability::Network)
    }) {
        TaskRisk::High
    } else if candidate.manifest.roles.iter().any(|role| {
        role.grant_ceiling
            .contains(&harness_contract::agent::AgentCapability::Write)
    }) {
        TaskRisk::Medium
    } else {
        TaskRisk::Low
    };
    let session_id = request.session_id.clone();
    let context = ApprovalContext {
        principal_id: format!(
            "session:{}",
            session_id.as_deref().unwrap_or("template-publisher")
        ),
        profile_id: "template-publish".to_string(),
        approval_profile: None,
        workspace_key: services.workspace_key().to_string(),
        session_id: session_id.clone(),
        turn_id: request
            .lineage
            .as_ref()
            .map(|lineage| lineage.turn_id.clone()),
        task_id: None,
        capability: "definition.template.publish".to_string(),
        invocation_id: Some(approval_id.clone()),
        execution_id: None,
        strategy_decision_ref: None,
        source_surface: request.surface.clone(),
        resource_targets: vec![template_id.clone()],
        effect: None,
        explicit_ask: true,
        effective_sandbox_posture: None,
        policy_revision: 0,
        requested_sandbox_posture: None,
    };
    let source = crate::ApprovalSource {
        kind: crate::ApprovalSourceKind::Session,
        session_id: session_id.clone(),
        agent_id: None,
        team_id: None,
        mission_id: request.mission_id.clone(),
        resource_ref: Some(template_id.clone()),
        review_ref: None,
        application: None,
    };
    services
        .approval_queue()
        .submit_scoped_with_policy(
            approval_id.clone(),
            crate::SubmitGlobalApprovalRequest {
                source,
                context,
                action: "definition.template.publish".to_string(),
                summary: format!("发布 AI 编排团队模板：{}", proposal.name),
                risk,
                domain: ApprovalDomain::System,
                blocks_execution: false,
                evidence_refs: vec![format!("template-candidate:{}", candidate.digest)],
                timeout_policy: crate::ApprovalTimeoutPolicy::Pending,
            },
            None,
            false,
            vec![ApprovalGrantScope::Once, ApprovalGrantScope::Global],
        )
        .map_err(|error| format!("template_approval_submit_failed:{error}"))?;
    let _ = services.event_store().append(crate::RuntimeEventInput {
        stream_id: format!("definition-template-candidate:{approval_id}"),
        scope: crate::RuntimeEventScope::Mission,
        kind: "definition.template.candidate.v1".to_string(),
        status: Some("pending_approval".to_string()),
        actor: Some(request_id.to_string()),
        refs: vec![crate::RuntimeEventRef {
            kind: "team_template".to_string(),
            id: template_id.clone(),
        }],
        payload: json!({
            "approval_id": approval_id,
            "manifest": candidate.manifest,
            "instructions": crate::team_template_candidate::normalized_team_instructions(
                &proposal.instructions,
            ),
            "digest": candidate.digest,
            "preview": candidate.preview,
        }),
    });
    let session_policy = session_id
        .as_deref()
        .and_then(|session_id| services.session_execution_policy(session_id));
    let router_profile = session_policy
        .as_ref()
        .map(|policy| policy.autonomy_profile)
        .unwrap_or(harness_contract::policy::AutonomyProfileId::Cautious);
    let router_decision = crate::approval_router::ApprovalRouter::resolve(
        router_profile,
        harness_contract::policy::ApprovalDomain::System,
        risk,
        false,
        true,
    );
    // Global template publication is committed by the canonical router actor
    // only for Autonomous/YOLO. Steward decisions fall through to the pending
    // human queue because a Global grant cannot be committed by a Steward.
    if router_decision == crate::approval_router::ApprovalDecision::AutoApprove {
        services
            .approval_queue()
            .decide_internal(ApprovalDecisionCommand {
                approval_id: approval_id.clone(),
                approved: true,
                skip: false,
                reason: format!("approval router {router_decision:?} for template publish"),
                scope: ApprovalGrantScope::Global,
                actor: ApprovalDecisionActor {
                    kind: ApprovalDecisionActorKind::Policy,
                    actor_id: "approval-router-auto".to_string(),
                },
                evidence_refs: vec![
                    "approval.router.auto".to_string(),
                    format!("approval.router.decision:{router_decision:?}"),
                ],
            })
            .map_err(|error| format!("template_router_approval_failed:{error}"))?;
        let published = services.publish_approved_template_candidate(&approval_id)?;
        return Ok(OperationOutcome {
            status: "completed".to_string(),
            execution: json!({
                "kind": "runtime.template_candidate",
                "status": "published",
                "approval_id": approval_id,
                "template_id": template_id,
                "digest": published.get("content_digest").cloned().unwrap_or_default(),
                "preview": candidate.preview,
            }),
            evidence: json!({ "refs": ["template-candidate"] }),
            guidance: "Template approved and published; it is part of the runnable team catalog."
                .to_string(),
        });
    }
    Ok(OperationOutcome {
        status: "pending_approval".to_string(),
        execution: json!({
            "kind": "runtime.template_candidate",
            "status": "pending_approval",
            "approval_id": approval_id,
            "template_id": template_id,
            "digest": candidate.digest,
            "preview": candidate.preview,
        }),
        evidence: json!({ "refs": ["approval"] }),
        guidance:
            "Template candidate awaits human approval; respond to the approval (definition.template.publish) to publish it."
                .to_string(),
    })
}

#[derive(Debug)]
struct OperationOutcome {
    status: String,
    execution: Value,
    evidence: Value,
    guidance: String,
}

async fn inspect(
    request: &RuntimeOrchestrationCommand,
    services: &RuntimeServices,
) -> Result<OperationOutcome, String> {
    let target = request.inspect_execution_id.as_deref();
    let graph = match target {
        Some(graph_id) => Some(
            services
                .execution_supervisor()
                .projection(graph_id)
                .await
                .map_err(|error| format!("inspect_projection_failed:{error}"))?,
        ),
        None => None,
    };
    let mut child_graphs = Vec::new();
    let mut inspected_graph_ids = target.into_iter().map(str::to_string).collect::<Vec<_>>();
    if let Some(graph_id) = target {
        for link in services
            .graph_state_store()
            .child_links_async(graph_id.to_string())
            .await
            .map_err(|error| format!("inspect_lineage_failed:{error}"))?
        {
            inspected_graph_ids.push(link.child_execution_id.clone());
            child_graphs.push(
                services
                    .execution_supervisor()
                    .projection(&link.child_execution_id)
                    .await
                    .map_err(|error| format!("inspect_child_projection_failed:{error}"))?,
            );
        }
    }
    let mut unresolved_conflicts = Vec::new();
    let mut artifact_refs = Vec::new();
    let mut team_board_revisions = std::collections::BTreeMap::new();
    for graph_id in inspected_graph_ids {
        if let Ok(team) = services.team_runtime().project(&graph_id) {
            if let Ok(state) = services
                .team_runtime()
                .working_state_for_graph(&team.team_id, &graph_id)
            {
                team_board_revisions.insert(team.team_id.clone(), state.board_revision);
                unresolved_conflicts.extend(
                    state
                        .entries
                        .iter()
                        .filter(|entry| {
                            matches!(
                                entry.kind,
                                crate::TeamWorkingStateKind::Conflict
                                    | crate::TeamWorkingStateKind::Unresolved
                            )
                        })
                        .map(|entry| format!("{}:{}", entry.entry_id, entry.summary)),
                );
                artifact_refs.extend(
                    state
                        .entries
                        .into_iter()
                        .flat_map(|entry| entry.artifact_refs),
                );
            }
        }
    }
    unresolved_conflicts.sort();
    unresolved_conflicts.dedup();
    artifact_refs.sort();
    artifact_refs.dedup();
    let team_templates = services
        .definition_registry()
        .runnable_team_catalog()
        .map_err(|error| format!("team_catalog_failed:{error}"))?
        .into_iter()
        .map(|entry| {
            format!(
                "{}@{}",
                entry.revision_ref.template_id.as_str(),
                entry.revision_ref.revision
            )
        })
        .collect::<Vec<_>>();
    let pending_approvals = services.approval_queue().pending().len();
    let snapshot_generation = graph.as_ref().map_or(0, |projection| projection.revision);
    let snapshot = RuntimeStateSnapshot {
        snapshot_generation,
        target_execution_id: target.map(str::to_string),
        graph,
        child_graphs,
        capability_recipes: recipe_catalog(),
        team_templates,
        permission_ceiling: request.constraints.permission_ceiling,
        pending_approvals,
        execution_health: serde_json::to_value(services.execution_health())
            .map_err(|error| format!("execution_health_encode_failed:{error}"))?,
        team_board_revisions,
        unresolved_conflicts,
        artifact_refs,
    };
    Ok(OperationOutcome {
        status: "inspected".to_string(),
        execution: serde_json::to_value(snapshot)
            .map_err(|error| format!("runtime_snapshot_encode_failed:{error}"))?,
        evidence: json!({"accepted": true, "executed": false, "operation": "inspect"}),
        guidance:
            "Use the current snapshot generation and graph revision when proposing a mutation."
                .to_string(),
    })
}

async fn propose(
    request_id: &str,
    request: &RuntimeOrchestrationCommand,
    plan: &RuntimeOrchestrationPlan,
    services: &RuntimeServices,
    parent_execution: Option<ExecutionParentBinding>,
    cancellation: Option<crate::CancellationToken>,
    submission_mode: OrchestrationSubmissionMode,
) -> Result<OperationOutcome, String> {
    let mut compiled =
        compile_orchestration_with_repair(request_id, request, plan, parent_execution, services)
            .map_err(|error| format!("semantic_compile_failed:{error}"))?;
    if submission_mode == OrchestrationSubmissionMode::AdmitBackground {
        compiled.graph.service_class =
            harness_contract::execution_graph::ExecutionServiceClass::Background;
    }
    let work_estimate = compiled.work_estimate.clone();
    let graph_id = compiled.graph.id.clone();
    match services.graph_state_store().load_async(&graph_id).await {
        Ok(existing) => {
            if mutation_applied(&existing, mutation_id(request)?) {
                return completed_projection(
                    request.operation,
                    services
                        .execution_supervisor()
                        .projection(&graph_id)
                        .await
                        .map_err(|error| format!("idempotent_projection_failed:{error}"))?,
                    None,
                    true,
                    services,
                );
            }
            return Err("mutation_identity_collision".to_string());
        }
        Err(ExecutionStateStoreError::NotFound(_)) => {}
        Err(error) => return Err(format!("proposal_identity_lookup_failed:{error}")),
    }
    let mut graph = services
        .compile_graph_agent_intents(compiled.graph)
        .map_err(|error| format!("agent_binding_compilation_failed:{error}"))?;
    collaboration_coordinator::prepare_program_admission(
        &mut graph,
        services.team_runtime().as_ref(),
    )?;
    if submission_mode == OrchestrationSubmissionMode::AdmitBackground {
        let registered = services
            .execution_supervisor()
            .register_graph(graph)
            .await
            .map_err(|error| format!("background_graph_registration_failed:{error}"))?;
        let receipt = services
            .execution_supervisor()
            .admit_registered(&registered.id)
            .await
            .map_err(|error| format!("background_graph_admission_failed:{error}"))?;
        return Ok(OperationOutcome {
            status: "admitted".to_string(),
            execution: json!({
                "type": "execution_graph_admission",
                "status": "admitted",
                "graph_id": registered.id,
                "receipt": receipt,
            }),
            evidence: json!({
                "accepted": true,
                "executed": false,
                "operation": request.operation.as_str(),
                "graph_id": registered.id,
                "service_class": "background",
            }),
            guidance: "Background work was durably admitted; continue the foreground Turn without waiting."
                .to_string(),
        });
    }
    let registered = services
        .execution_supervisor()
        .register_graph(graph)
        .await
        .map_err(|error| format!("graph_registration_failed:{error}"))?;
    let run = services
        .execution_supervisor()
        .admit_registered_and_wait_terminal(&registered.id);
    let (_, report) = await_with_cancellation(run, cancellation, services, &graph_id).await?;
    collaboration_coordinator::reconcile_terminal_program(&graph_id, services).await?;
    let projection = services
        .execution_supervisor()
        .projection(&graph_id)
        .await
        .map_err(|error| format!("execution_projection_failed:{error}"))?;
    let mut outcome =
        completed_projection(request.operation, projection, Some(report), false, services)?;
    if let Some(evidence) = outcome.evidence.as_object_mut() {
        evidence.insert(
            "model_work_estimate".to_string(),
            serde_json::to_value(work_estimate)
                .map_err(|error| format!("model_work_estimate_encode_failed:{error}"))?,
        );
        if !compiled.repairs.is_empty() {
            evidence.insert(
                "compile_repairs".to_string(),
                serde_json::to_value(&compiled.repairs)
                    .map_err(|error| format!("compile_repairs_encode_failed:{error}"))?,
            );
        }
    }
    Ok(outcome)
}

/// Compile a proposal with at most two Runtime-owned repairs before failing.
/// Repair is kernel behavior: it may attach a session evidence lease or
/// re-bind a missing Mission, but it never widens permissions or invents
/// model intent.
fn compile_orchestration_with_repair(
    request_id: &str,
    request: &RuntimeOrchestrationCommand,
    plan: &RuntimeOrchestrationPlan,
    parent_execution: Option<ExecutionParentBinding>,
    services: &RuntimeServices,
) -> Result<CompiledOrchestration, String> {
    let mut attempt = request.clone();
    let mut last_error = String::new();
    // Framework-level capacity floor: operators declare provider concurrency
    // (providers support thousands of concurrent requests). The estimator
    // uses max(configured, observed pool peak), clamped to the effective
    // provider limit, so parallel admission is not blocked by a cold
    // observation of zero in-flight requests.
    let configured_provider_concurrency = std::env::var("COWD_PROVIDER_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(64)
        .max(1);
    let mut effective_plan = plan.clone();
    {
        let snapshot = &mut effective_plan.execution_decision.strategy.resource_snapshot;
        let effective = usize::from(snapshot.provider_effective_limit).max(1);
        snapshot.provider_concurrency = snapshot
            .provider_concurrency
            .max(u16::try_from(configured_provider_concurrency.min(effective)).unwrap_or(u16::MAX));
    }
    for round in 0..=2 {
        match compiler::compile_orchestration_with_capacity(
            request_id,
            &attempt,
            &effective_plan,
            parent_execution.as_ref().cloned(),
            Some(services.team_runtime().as_ref()),
            Some(&services.execution_capacity_profile().team_snapshot()),
        ) {
            Ok(compiled) => return Ok(compiled),
            Err(error) => {
                last_error = error.to_string();
                if round == 2
                    || !repair_semantic_compilation(
                        &mut attempt,
                        services.workspace_root(),
                        &last_error,
                    )
                {
                    return Err(last_error);
                }
            }
        }
    }
    Err(last_error)
}

fn repair_semantic_compilation(
    request: &mut RuntimeOrchestrationCommand,
    workspace_root: &std::path::Path,
    error: &str,
) -> bool {
    if error.contains("Team mission not found") {
        let key = storage::StorageScope::workspace_key_for_root(workspace_root);
        let default_mission = format!("mission-default-{key}");
        if request.mission_id.as_deref() != Some(default_mission.as_str()) {
            request.mission_id = Some(default_mission);
            return true;
        }
        return false;
    }
    if error.contains("Team execution requires at least one Runtime-cropped")
        && request.session_id.is_some()
    {
        let session_id = request.session_id.clone().unwrap_or_default();
        let scope = format!("session:{session_id}");
        if let Some(proposal) = request.proposal.as_mut() {
            for node in &mut proposal.nodes {
                if !node
                    .resource_scopes
                    .iter()
                    .any(|item| item.starts_with("session:"))
                {
                    node.resource_scopes.push(scope.clone());
                }
            }
        }
        return true;
    }
    false
}

async fn revise(
    request_id: &str,
    request: &RuntimeOrchestrationCommand,
    plan: &RuntimeOrchestrationPlan,
    services: &RuntimeServices,
    cancellation: Option<crate::CancellationToken>,
) -> Result<OperationOutcome, String> {
    let proposal = request
        .proposal
        .as_ref()
        .ok_or_else(|| "revise_missing_proposal".to_string())?;
    let graph_id = proposal
        .target_execution_id
        .as_deref()
        .ok_or_else(|| "revise_missing_target".to_string())?;
    let requested_base_revision = proposal
        .expected_revision
        .ok_or_else(|| "revise_missing_expected_revision".to_string())?;
    let mut report = None;
    for attempt in 0..MAX_REVISION_CAS_ATTEMPTS {
        // Every retry starts from canonical durable state and recompiles the
        // semantic mutation against that exact topology. Never replay a
        // previously compiled physical delta after a stale CAS.
        let graph = services
            .graph_state_store()
            .load_async(graph_id)
            .await
            .map_err(|error| format!("revise_target_load_failed:{error}"))?;
        if mutation_applied(&graph, &proposal.mutation_id) {
            return completed_projection(
                request.operation,
                services
                    .execution_supervisor()
                    .projection(graph_id)
                    .await
                    .map_err(|error| format!("idempotent_projection_failed:{error}"))?,
                None,
                true,
                services,
            );
        }
        if graph.revision < requested_base_revision {
            return Err(format!(
                "semantic_revision_base_is_in_the_future:requested={requested_base_revision}:actual={}",
                graph.revision
            ));
        }
        // A CollaborationIntentPatch is fenced both by its Program revision
        // (checked when compiled) and by this root-graph revision.  Generic
        // semantic revisions may be safely recompiled after an unrelated
        // graph transition, but a live-program patch must never silently
        // rebase: its source Agent made the decision against an exact durable
        // topology.  The prefix is private to `compile_collaboration_intent_patch`; model
        // JSON cannot acquire a more permissive path by choosing it.
        if proposal.mutation_id.starts_with("program-patch:")
            && graph.revision != requested_base_revision
        {
            return Err(format!(
                "patch_graph_revision_conflict:requested={requested_base_revision}:actual={}",
                graph.revision
            ));
        }
        let existing_ids = graph
            .nodes
            .iter()
            .map(|node| node.id.clone())
            .collect::<BTreeSet<_>>();
        let existing_semantic_node_instances = graph
            .orchestration
            .as_ref()
            .and_then(|metadata| metadata.collaboration_program.as_ref())
            .map_or_else(BTreeMap::new, |program| {
                program.semantic_node_instances.clone()
            });
        let frozen_capacity = if graph
            .orchestration
            .as_ref()
            .and_then(|metadata| metadata.collaboration_program.as_ref())
            .is_some()
        {
            capacity_snapshot_from_existing_program(&graph)?
        } else {
            services.execution_capacity_profile().team_snapshot()
        };
        if let Some(program) = graph
            .orchestration
            .as_ref()
            .and_then(|metadata| metadata.collaboration_program.as_ref())
        {
            let retired = proposal
                .retired_collaboration_instance_ids
                .iter()
                .filter(|instance_id| {
                    program
                        .team_instances
                        .iter()
                        .any(|instance| &instance.instance_id == *instance_id)
                })
                .count();
            let added = proposal
                .nodes
                .iter()
                .filter(|node| node.recipe == CapabilityRecipeId::Team)
                .try_fold(0usize, |total, node| {
                    total.checked_add(usize::from(node.multiplicity))
                })
                .ok_or_else(|| "program_team_count_overflow".to_string())?;
            let resulting = program
                .team_instances
                .len()
                .checked_sub(retired)
                .and_then(|active| active.checked_add(added))
                .ok_or_else(|| "program_team_count_overflow".to_string())?;
            if resulting > frozen_capacity.max_program_teams {
                return Err(format!(
                    "program_team_count_exceeds_capacity:{resulting}>{}",
                    frozen_capacity.max_program_teams
                ));
            }
        }
        let mut revision_repairs = Vec::new();
        let mut mutation = compiler::compile_graph_mutation(
            request_id,
            request,
            plan,
            proposal,
            graph_id,
            graph.parent_execution.as_ref(),
            services.team_runtime().as_ref(),
            Some(&frozen_capacity),
            &existing_semantic_node_instances,
            &mut revision_repairs,
        )
        .map_err(|error| format!("semantic_revision_compile_failed:{error}"))?;
        if !proposal.retired_collaboration_instance_ids.is_empty() {
            let existing_program = graph
                .orchestration
                .as_ref()
                .and_then(|metadata| metadata.collaboration_program.as_ref())
                .ok_or_else(|| "semantic_revision_split_target_has_no_program".to_string())?;
            let replacement_node_ids = proposal
                .nodes
                .iter()
                .filter(|node| node.recipe == CapabilityRecipeId::Team)
                .flat_map(|node| {
                    mutation
                        .semantic_node_instances
                        .get(&node.node_id)
                        .into_iter()
                        .flatten()
                        .cloned()
                })
                .collect::<Vec<_>>();
            mutation.edges.extend(
                collaboration_coordinator::split_replacement_outgoing_graph_edges(
                    &graph,
                    existing_program,
                    &proposal.retired_collaboration_instance_ids,
                    &replacement_node_ids,
                )
                .map_err(|error| format!("semantic_revision_split_relations_failed:{error}"))?,
            );
        }
        if let Some(conflict) = mutation
            .nodes
            .iter()
            .find(|node| existing_ids.contains(&node.id))
        {
            return Err(format!(
                "semantic_revision_noncommutative_conflict:{}",
                conflict.id
            ));
        }
        services
            .compile_agent_task_nodes(&mut mutation.nodes)
            .map_err(|error| format!("agent_binding_compilation_failed:{error}"))?;
        let mut candidate_graph = graph.clone();
        candidate_graph.nodes.extend(mutation.nodes.clone());
        candidate_graph.edges.extend(mutation.edges.clone());
        compiler::apply_strategy_estimates(&mut candidate_graph, plan);
        let estimate = compiler::estimate_work_graph(&candidate_graph, plan, proposal);
        compiler::ensure_positive_work_lift(&candidate_graph, &estimate)
            .map_err(|error| format!("semantic_revision_negative_lift:{error}"))?;
        let estimated_work = candidate_graph
            .nodes
            .into_iter()
            .filter_map(|node| node.work.map(|work| (node.id, work)))
            .collect::<std::collections::BTreeMap<_, _>>();
        for node in &mut mutation.nodes {
            if let Some(work) = estimated_work.get(&node.id) {
                node.work = Some(work.clone());
            }
        }
        let completion = compiler::materialize_completion(
            &proposal.completion,
            &mutation.semantic_node_instances,
            &proposal.nodes,
        );
        let mut collaboration_program = compiler::collaboration_program_from_proposal(
            proposal,
            Some(&mutation.semantic_node_instances),
        )
        .map_err(|error| format!("semantic_revision_program_failed:{error}"))?;
        if let Some(delta) = collaboration_program.as_mut() {
            collaboration_coordinator::prepare_program_revision_admission(
                &graph,
                delta,
                mutation.nodes.clone(),
                services.team_runtime().as_ref(),
            )
            .map_err(|error| format!("semantic_revision_admission_failed:{error}"))?;
        }
        if let (Some(delta), Some(existing)) = (
            collaboration_program.as_mut(),
            graph
                .orchestration
                .as_ref()
                .and_then(|metadata| metadata.collaboration_program.as_ref()),
        ) {
            if !proposal.retired_collaboration_instance_ids.is_empty() {
                let replacement_instance_ids = delta
                    .team_instances
                    .iter()
                    .map(|instance| instance.instance_id.clone())
                    .collect::<Vec<_>>();
                for edge in collaboration_coordinator::split_replacement_outgoing_program_edges(
                    existing,
                    &proposal.retired_collaboration_instance_ids,
                    &replacement_instance_ids,
                ) {
                    if !delta
                        .edges
                        .iter()
                        .any(|candidate| candidate.edge_id == edge.edge_id)
                    {
                        delta.edges.push(edge);
                    }
                }
            }
            // A patch may add a reviewer/aggregator that consumes a Team
            // already admitted by the root program. Represent that relation
            // explicitly instead of silently degrading it to a prose prompt
            // or a generic graph dependency.
            for consumer in proposal
                .nodes
                .iter()
                .filter(|node| node.recipe == CapabilityRecipeId::Team)
            {
                let consumers = delta
                    .team_instances
                    .iter()
                    .filter(|instance| instance.semantic_node_id == consumer.node_id)
                    .map(|instance| instance.instance_id.clone())
                    .collect::<Vec<_>>();
                for producer_semantic_id in &consumer.depends_on {
                    let producers = existing
                        .team_instances
                        .iter()
                        .filter(|instance| instance.semantic_node_id == *producer_semantic_id)
                        .map(|instance| instance.instance_id.clone())
                        .collect::<Vec<_>>();
                    for from in &producers {
                        for to in &consumers {
                            let edge_id = format!("{from}->{to}");
                            if !delta.edges.iter().any(|edge| edge.edge_id == edge_id) {
                                delta.edges.push(
                                    harness_contract::execution_graph::CollaborationProgramEdge {
                                        edge_id,
                                        from: from.clone(),
                                        to: to.clone(),
                                        kind: harness_contract::execution_graph::CollaborationEdgeKind::Handoff,
                                        input_contract: harness_contract::execution_graph::CrossTeamInputContract {
                                            required_artifact_kinds: Vec::new(),
                                            required_fact_kinds: vec![
                                                harness_contract::acceptance::TerminalFactKind::ObservedEvidence,
                                                harness_contract::acceptance::TerminalFactKind::AcceptanceVerdict,
                                            ],
                                            require_committed_effect: false,
                                            require_satisfied_acceptance: false,
                                        },
                                        state: Default::default(),
                                        delivery_receipt: None,
                                        claim_receipt: None,
                                    },
                                );
                            }
                        }
                    }
                }
            }
        }
        let run = services.execution_supervisor().revise_semantic_graph(
            graph_id,
            graph.revision,
            mutation.nodes,
            mutation.edges,
            proposal.reason.clone(),
            proposal.mutation_id.clone(),
            completion,
            collaboration_program,
            proposal.collaboration_escalation.clone(),
            proposal.retired_collaboration_instance_ids.clone(),
        );
        let outcome = if let Some(cancellation) = cancellation.as_ref() {
            tokio::select! {
                outcome = run => outcome,
                () = cancellation.cancelled() => {
                    cancel_graph(services, graph_id).await?;
                    return Err("parent_execution_cancelled".to_string());
                }
            }
        } else {
            run.await
        };
        match outcome {
            Ok((_, attempt_report)) => {
                report = Some(attempt_report);
                break;
            }
            Err(error) if semantic_revision_is_stale(&error) => {
                if !semantic_revision_may_retry(&error, attempt + 1) {
                    return Err(format!(
                        "semantic_revision_conflict_exhausted:attempts={MAX_REVISION_CAS_ATTEMPTS}:{error}"
                    ));
                }
            }
            Err(error) => return Err(format!("execution_failed:{error}")),
        }
    }
    let report = report.ok_or_else(|| "semantic_revision_missing_report".to_string())?;
    let projection = services
        .execution_supervisor()
        .projection(graph_id)
        .await
        .map_err(|error| format!("revision_projection_failed:{error}"))?;
    completed_projection(request.operation, projection, Some(report), false, services)
}

fn semantic_revision_is_stale(error: &ExecutionRunnerError) -> bool {
    matches!(
        error,
        ExecutionRunnerError::Commit(ExecutionCommitError::StaleRevision { .. })
            | ExecutionRunnerError::Commit(ExecutionCommitError::EventStore(
                crate::RuntimeEventStoreError::StaleRevision { .. }
            ))
    )
}

fn semantic_revision_may_retry(error: &ExecutionRunnerError, attempts_started: usize) -> bool {
    semantic_revision_is_stale(error) && attempts_started < MAX_REVISION_CAS_ATTEMPTS
}

async fn control(
    request: &RuntimeOrchestrationCommand,
    services: &RuntimeServices,
) -> Result<OperationOutcome, String> {
    let control = request
        .control
        .as_ref()
        .ok_or_else(|| "control_payload_missing".to_string())?;
    let command = match control.action {
        RuntimeControlKind::Pause => ExecutionGraphCommand::Pause {
            expected_revision: control.expected_revision,
            reason: control.reason.clone(),
        },
        RuntimeControlKind::Resume => ExecutionGraphCommand::Resume {
            expected_revision: control.expected_revision,
        },
        RuntimeControlKind::Cancel => {
            if let Some(node_id) = control.target_node_id.clone() {
                ExecutionGraphCommand::CancelNode {
                    expected_revision: control.expected_revision,
                    node_id,
                    reason: control.reason.clone(),
                }
            } else {
                ExecutionGraphCommand::Cancel {
                    expected_revision: control.expected_revision,
                    reason: control.reason.clone(),
                }
            }
        }
    };
    services
        .execution_supervisor()
        .command_graph(&control.target_execution_id, command)
        .await
        .map_err(|error| format!("runtime_control_failed:{error}"))?;
    let projection = services
        .execution_supervisor()
        .projection(&control.target_execution_id)
        .await
        .map_err(|error| format!("control_projection_failed:{error}"))?;
    completed_projection(request.operation, projection, None, false, services)
}

async fn await_with_cancellation<F>(
    future: F,
    cancellation: Option<crate::CancellationToken>,
    services: &RuntimeServices,
    graph_id: &str,
) -> Result<(crate::ExecutionGraphHostReceipt, ExecutionRunReport), String>
where
    F: std::future::Future<
        Output = Result<
            (crate::ExecutionGraphHostReceipt, ExecutionRunReport),
            crate::execution_core::ExecutionRunnerError,
        >,
    >,
{
    let Some(cancellation) = cancellation else {
        return future
            .await
            .map_err(|error| format!("execution_failed:{error}"));
    };
    tokio::select! {
        outcome = future => outcome.map_err(|error| format!("execution_failed:{error}")),
        () = cancellation.cancelled() => {
            cancel_graph(services, graph_id).await?;
            Err("parent_execution_cancelled".to_string())
        }
    }
}

async fn cancel_graph(services: &RuntimeServices, graph_id: &str) -> Result<(), String> {
    let graph = services
        .graph_state_store()
        .load_async(graph_id)
        .await
        .map_err(|error| format!("cancel_target_load_failed:{error}"))?;
    if graph
        .node_statuses
        .values()
        .all(|status| status.is_terminal())
    {
        return Ok(());
    }
    services
        .execution_supervisor()
        .command_graph(
            graph_id,
            ExecutionGraphCommand::Cancel {
                expected_revision: graph.revision,
                reason: "parent execution cancellation propagated to semantic graph".to_string(),
            },
        )
        .await
        .map_err(|error| format!("cancel_graph_failed:{error}"))?;
    Ok(())
}

fn completed_projection(
    operation: RuntimeOrchestrationOperation,
    projection: ExecutionGraphProjection,
    report: Option<ExecutionRunReport>,
    reused: bool,
    services: &RuntimeServices,
) -> Result<OperationOutcome, String> {
    let mut completion_findings = completion_findings(&projection);
    let team_assessment = assess_team_subgraphs(&projection, services);
    completion_findings.extend(team_assessment.findings.iter().cloned());
    let (collaboration_program, collaboration_diagnostics) =
        collaboration_program_projection(&projection);
    completion_findings.extend(collaboration_diagnostics.iter().map(|diagnostic| {
        format!(
            "collaboration_terminal_diagnostic:{}:{}",
            diagnostic.team_instance_id, diagnostic.code
        )
    }));
    completion_findings.sort();
    completion_findings.dedup();
    let status = if graph_status(&projection) == "completed" && !completion_findings.is_empty() {
        "blocked"
    } else {
        graph_status(&projection)
    };
    let graph_id = projection.graph_id.clone();
    let terminal_result_ref = projection
        .nodes
        .iter()
        .rev()
        .find_map(|node| node.result_ref.clone());
    Ok(OperationOutcome {
        status: status.to_string(),
        execution: json!({
            "type": "execution_graph_run",
            "status": status,
            "projection": projection,
            "report": report,
            "terminal_result_ref": terminal_result_ref,
            "team_subgraphs": team_assessment.teams,
            "team_terminals": team_assessment.team_terminals,
            "collaboration_program": collaboration_program,
            "collaboration_diagnostics": collaboration_diagnostics,
            "completion_findings": completion_findings,
        }),
        evidence: json!({
            "accepted": status == "completed",
            "executed": operation != RuntimeOrchestrationOperation::Inspect,
            "operation": operation.as_str(),
            "graph_id": graph_id,
            "reused": reused,
            "completion_findings": completion_findings,
            "team_ids": team_assessment.team_ids,
            "working_state_verified": team_assessment.has_teams.then_some(team_assessment.working_state_verified),
            "focus_overlap_verified": team_assessment.has_teams.then_some(team_assessment.focus_overlap_verified),
            "focus_overlap_exceeded": team_assessment.has_teams.then_some(team_assessment.focus_overlap_exceeded),
            "committed_write": team_assessment.committed_write,
            "committed_write_paths": team_assessment.committed_write_paths,
            "write_attempt_paths": team_assessment.usage.runtime_write_attempt_paths,
            "observed_acceptance": team_assessment.usage.observed_acceptance,
            "child_usage": {
                "input_tokens": team_assessment.usage.input_tokens,
                "output_tokens": team_assessment.usage.output_tokens,
                "cached_tokens": team_assessment.usage.cached_tokens,
                "tool_calls": team_assessment.usage.tool_calls,
                "duplicate_tool_calls": team_assessment.usage.duplicate_tool_calls,
                "max_tool_concurrency_observed": team_assessment.usage.max_tool_concurrency_observed,
                "parallel_tool_batches": team_assessment.usage.parallel_tool_batches,
            },
        }),
        guidance: if status == "completed" {
            "Continue from the checked terminal synthesis and durable evidence.".to_string()
        } else {
            "Inspect the graph revision and unresolved nodes before proposing a bounded revision."
                .to_string()
        },
    })
}

/// Render the sole durable collaboration terminal into the bounded operation
/// projection.  The conversation host and Surface consume this carrier; they
/// must not reconstruct success from a tool transcript or infer a failure
/// from a physical node id.  Child graph details remain behind their governed
/// evidence references.
fn collaboration_program_projection(
    projection: &ExecutionGraphProjection,
) -> (
    Option<Value>,
    Vec<harness_contract::execution_graph::CollaborationDiagnostic>,
) {
    use harness_contract::execution_graph::{
        CollaborationDiagnostic, TeamAdmissionState, TeamExecutionTerminal,
    };

    let Some(program) = projection
        .orchestration
        .as_ref()
        .and_then(|metadata| metadata.collaboration_program.as_ref())
    else {
        return (None, Vec::new());
    };

    let mut completed_required_instance_ids = Vec::new();
    let mut diagnostics = Vec::new();
    for (instance_index, instance) in program.team_instances.iter().enumerate() {
        let same_semantic_index = program.team_instances[..instance_index]
            .iter()
            .filter(|candidate| candidate.semantic_node_id == instance.semantic_node_id)
            .count();
        let execution_node_id = program
            .semantic_node_instances
            .get(&instance.semantic_node_id)
            .and_then(|nodes| nodes.get(same_semantic_index))
            .cloned()
            .unwrap_or_else(|| format!("unmapped:{}", instance.instance_id));
        let node = projection
            .nodes
            .iter()
            .find(|node| node.node_id == execution_node_id);
        let obligation = program
            .control
            .obligations
            .iter()
            .find(|obligation| obligation.instance_id == instance.instance_id);
        let terminal: Option<&TeamExecutionTerminal> =
            obligation.and_then(|item| item.terminal.as_ref());
        let node_status = terminal
            .map(|terminal| terminal.node_status)
            .or_else(|| node.map(|node| node.status))
            .unwrap_or(ExecutionNodeStatus::Planned);
        let admitted = obligation.is_some_and(|item| item.state == TeamAdmissionState::Admitted);
        if instance.required && admitted && node_status == ExecutionNodeStatus::Completed {
            completed_required_instance_ids.push(instance.instance_id.clone());
            continue;
        }
        // A running Program is not a failure card.  Emit a diagnostic only
        // after the Program/node/admission obligation itself is terminal;
        // otherwise the Surface would turn ordinary progress into a false
        // error and tempt the parent model to re-plan prematurely.
        if !program.control.lifecycle.is_terminal()
            && !node_status.is_terminal()
            && !obligation.is_some_and(|item| item.state.is_terminal())
        {
            continue;
        }

        let failure = terminal
            .and_then(|terminal| {
                terminal
                    .failure_kind
                    .clone()
                    .zip(terminal.failure_message.clone())
            })
            .or_else(|| {
                node.and_then(|node| {
                    node.failure
                        .as_ref()
                        .map(|failure| (failure.kind.clone(), failure.message.clone()))
                })
            });
        let (failure_kind, failure_message) = failure
            .map(|(kind, message)| (Some(kind), Some(message)))
            .unwrap_or_else(|| {
                let reason = obligation
                    .and_then(|item| item.reason_kind.clone())
                    .unwrap_or_else(|| format!("team_node_{node_status:?}").to_ascii_lowercase());
                (
                    Some(reason),
                    Some(format!(
                        "Team instance `{}` did not reach a completed terminal (status: {node_status:?})",
                        instance.instance_id
                    )),
                )
            });
        diagnostics.push(CollaborationDiagnostic {
            code: if admitted {
                "team_execution_not_completed".to_string()
            } else {
                "team_admission_not_completed".to_string()
            },
            program_id: program.program_id.clone(),
            team_instance_id: instance.instance_id.clone(),
            semantic_node_id: instance.semantic_node_id.clone(),
            execution_node_id,
            child_graph_ref: obligation.and_then(|item| item.child_graph_ref.clone()),
            node_status,
            failure_kind,
            failure_message,
            retryable: terminal.is_some_and(|terminal| terminal.retryable)
                || node
                    .and_then(|node| node.failure.as_ref())
                    .is_some_and(|failure| failure.retryable),
            evidence_refs: terminal
                .map(|terminal| terminal.evidence_refs.clone())
                .or_else(|| node.map(|node| node.evidence_refs.clone()))
                .unwrap_or_default(),
            next_action: program.control.next_action.clone(),
        });
    }
    completed_required_instance_ids.sort();
    let result = json!({
        "program_id": program.program_id,
        "revision": program.revision,
        "lifecycle": program.control.lifecycle,
        "blocker_ref": program.control.blocker_ref,
        "next_action": program.control.next_action,
        "required_team_count": program.required_team_count,
        "completed_required_instance_ids": completed_required_instance_ids,
        "terminal_diagnostics": diagnostics,
        "obligations": program.control.obligations,
    });
    (Some(result), diagnostics)
}

#[derive(Debug, Default)]
struct TeamSubgraphAssessment {
    has_teams: bool,
    working_state_verified: bool,
    focus_overlap_verified: bool,
    focus_overlap_exceeded: bool,
    committed_write: bool,
    committed_write_paths: BTreeSet<String>,
    usage: ExecutionUsage,
    team_ids: Vec<String>,
    teams: Vec<Value>,
    team_terminals: Vec<Value>,
    findings: Vec<String>,
}

fn assess_team_subgraphs(
    projection: &ExecutionGraphProjection,
    services: &RuntimeServices,
) -> TeamSubgraphAssessment {
    let mut assessment = TeamSubgraphAssessment {
        working_state_verified: true,
        focus_overlap_verified: true,
        ..TeamSubgraphAssessment::default()
    };
    let parent_graph = services.graph_state_store().load(&projection.graph_id).ok();
    for node in projection.nodes.iter().filter(|node| {
        node.executor_kind == compiler::TEAM_SUBGRAPH_EXECUTOR
            && node.status == ExecutionNodeStatus::Completed
    }) {
        assessment.has_teams = true;
        let payload_ref = parent_graph.as_ref().and_then(|graph| {
            graph
                .nodes
                .iter()
                .find(|candidate| candidate.id == node.node_id)
                .map(|candidate| candidate.payload_ref.as_str())
        });
        let request = match payload_ref.and_then(|payload| {
            serde_json::from_str::<harness_contract::team::TeamInstantiationRequest>(payload).ok()
        }) {
            Some(request) => request,
            None => {
                assessment.working_state_verified = false;
                assessment.findings.push(format!(
                    "team_subgraph_payload_unavailable:{}",
                    node.node_id
                ));
                continue;
            }
        };
        let team_id = request.team_id;
        let child_graph_id = format!("team-graph:{team_id}");
        assessment.usage.input_tokens = assessment
            .usage
            .input_tokens
            .saturating_add(node.usage.input_tokens);
        assessment.usage.output_tokens = assessment
            .usage
            .output_tokens
            .saturating_add(node.usage.output_tokens);
        assessment.usage.cached_tokens = assessment
            .usage
            .cached_tokens
            .saturating_add(node.usage.cached_tokens);
        assessment.usage.tool_calls = assessment
            .usage
            .tool_calls
            .saturating_add(node.usage.tool_calls);
        assessment.usage.duplicate_tool_calls = assessment
            .usage
            .duplicate_tool_calls
            .saturating_add(node.usage.duplicate_tool_calls);
        assessment.usage.max_tool_concurrency_observed = assessment
            .usage
            .max_tool_concurrency_observed
            .max(node.usage.max_tool_concurrency_observed);
        assessment.usage.parallel_tool_batches = assessment
            .usage
            .parallel_tool_batches
            .saturating_add(node.usage.parallel_tool_batches);
        assessment
            .usage
            .runtime_write_attempt_paths
            .extend(node.usage.runtime_write_attempt_paths.iter().cloned());
        assessment
            .usage
            .observed_acceptance
            .merge_from(&node.usage.observed_acceptance);
        let (team_committed_write_paths, invalid_change_receipts) =
            committed_change_paths(&node.evidence_refs);
        assessment.findings.extend(
            invalid_change_receipts
                .into_iter()
                .map(|id| format!("team_runtime_change_receipt_invalid:{team_id}:{id}")),
        );
        assessment.committed_write |= !team_committed_write_paths.is_empty();
        assessment
            .committed_write_paths
            .extend(team_committed_write_paths.iter().cloned());
        assessment.team_ids.push(team_id.clone());
        let working_state = match services
            .team_runtime()
            .working_state_for_graph(&team_id, &child_graph_id)
        {
            Ok(state) if !state.entries.is_empty() => state,
            Ok(_) => {
                assessment.working_state_verified = false;
                assessment.findings.push(format!(
                    "team_terminal_missing_committed_working_state:{team_id}"
                ));
                continue;
            }
            Err(error) => {
                assessment.working_state_verified = false;
                assessment
                    .findings
                    .push(format!("team_working_state_unavailable:{team_id}:{error}"));
                continue;
            }
        };
        let overlap = working_state.focus_overlap_assessment();
        assessment.focus_overlap_verified &= overlap.observed;
        assessment.focus_overlap_exceeded |= overlap.exceeded;
        let child_graph = services.graph_state_store().load(&child_graph_id);
        let materialization = child_graph
            .as_ref()
            .map_err(ToString::to_string)
            .and_then(|graph| working_state.verify_completed_graph(graph));
        if let Err(error) = &materialization {
            assessment.working_state_verified = false;
            assessment.findings.push(format!(
                "team_working_state_not_materialized:{team_id}:{error}"
            ));
        }
        if overlap.exceeded {
            assessment.findings.push(format!(
                "team_focus_overlap_budget_exceeded:{team_id}:{}bp>{}bp",
                overlap.maximum_overlap_bp, overlap.allowed_overlap_bp
            ));
        }
        assessment.teams.push(json!({
            "team_id": team_id,
            "graph_id": child_graph_id,
            "board_revision": working_state.board_revision,
            "entry_count": working_state.entries.len(),
            "working_state_verified": materialization.is_ok(),
            "focus_overlap_assessment": overlap,
            "committed_write": !team_committed_write_paths.is_empty(),
            "committed_write_paths": team_committed_write_paths,
            "usage": node.usage,
        }));
        if let Ok(child_graph) = child_graph {
            let mut delivery_envelope = child_graph.delivery_envelope;
            if let Some(envelope) = delivery_envelope.as_mut() {
                // The child graph is the durable source of the detailed
                // receipts. Parent/model transport needs only a compact typed
                // terminal identity and satisfaction proof; copying every
                // branch/artifact/obligation can exceed the ToolResult budget
                // and split the JSON before the parent can validate it.
                envelope.branch_terminals.clear();
                envelope.verified_receipts.clear();
                envelope.verified_artifacts.clear();
                envelope.verified_effects.clear();
                if envelope.delivery_status == harness_contract::outcome::DeliveryStatus::Satisfied
                    && envelope.unresolved.is_empty()
                    && envelope.coverage.required_obligation_ids
                        == envelope.coverage.satisfied_obligation_ids
                {
                    envelope.coverage.required_obligation_ids.clear();
                    envelope.coverage.satisfied_obligation_ids.clear();
                }
            }
            // This receipt is the root synthesizer's canonical semantic input,
            // not a UI preview. Keep the complete Team result; context packing
            // may stage large byte payloads, but it must never silently change
            // their meaning by slicing at an arbitrary character boundary.
            let terminal_summary = node.summary.clone();
            let terminal_summary_kind = child_graph
                .nodes
                .iter()
                .find(|node| node.kind == ExecutionNodeKind::Synthesize)
                .and_then(|node| child_graph.node_results.get(&node.id))
                .and_then(|result| result.summary.as_deref())
                .filter(|summary| summary.starts_with("# Verified Team evidence bundle"))
                .map(|_| "verified_team_evidence_bundle");
            assessment.team_terminals.push(json!({
                "team_id": team_id,
                "graph_id": child_graph_id,
                "working_state_verified": materialization.is_ok(),
                "terminal_summary": terminal_summary,
                "terminal_summary_kind": terminal_summary_kind,
                "delivery_envelope": delivery_envelope,
                "terminal_presentation": child_graph.terminal_presentation,
            }));
        }
    }
    assessment.team_ids.sort();
    assessment.team_ids.dedup();
    assessment.usage.runtime_write_attempt_paths.sort();
    assessment.usage.runtime_write_attempt_paths.dedup();
    assessment
}

fn committed_change_paths(
    evidence_refs: &[harness_contract::context::EvidenceAccessRef],
) -> (BTreeSet<String>, Vec<String>) {
    let mut paths = BTreeSet::new();
    let mut invalid = Vec::new();
    for evidence in evidence_refs {
        if evidence.evidence_ref.ref_type != "runtime_change" {
            continue;
        }
        let Ok(change) = serde_json::from_str::<harness_contract::agent::AgentChangeReceipt>(
            &evidence.evidence_ref.id,
        ) else {
            invalid.push(evidence.evidence_ref.id.clone());
            continue;
        };
        if change.path.trim().is_empty() {
            invalid.push(evidence.evidence_ref.id.clone());
        } else {
            paths.insert(change.path);
        }
    }
    (paths, invalid)
}

fn completion_findings(projection: &ExecutionGraphProjection) -> Vec<String> {
    let Some(metadata) = projection.orchestration.as_ref() else {
        return Vec::new();
    };
    let (_, collaboration_diagnostics) = collaboration_program_projection(projection);
    let mut findings = Vec::new();
    for required in &metadata.completion.required_node_ids {
        if projection
            .nodes
            .iter()
            .find(|node| node.node_id == *required)
            .is_none_or(|node| node.status != ExecutionNodeStatus::Completed)
        {
            if let Some(diagnostic) = collaboration_diagnostics
                .iter()
                .find(|diagnostic| diagnostic.execution_node_id == *required)
            {
                findings.push(format!(
                    "collaboration_terminal_diagnostic:{}:{}",
                    diagnostic.team_instance_id, diagnostic.code
                ));
            } else {
                findings.push(format!("required_node_not_completed:{required}"));
            }
        }
    }
    for artifact in &metadata.completion.required_artifact_kinds {
        let materialized = projection.nodes.iter().any(|node| {
            node.status == ExecutionNodeStatus::Completed
                && node
                    .acceptance
                    .required_evidence
                    .iter()
                    .any(|required| required == artifact)
                && (node.result_ref.is_some() || !node.evidence_refs.is_empty())
        });
        if !materialized {
            findings.push(format!("required_artifact_not_materialized:{artifact}"));
        }
    }
    if !metadata.completion.allow_unresolved_conflicts
        && projection.nodes.iter().any(|node| {
            node.result_ref
                .as_deref()
                .is_some_and(|reference| reference.ends_with(":unresolved"))
        })
    {
        findings.push("unresolved_conflict_rejected".to_string());
    }
    findings
}

fn graph_status(projection: &ExecutionGraphProjection) -> &'static str {
    let required = |node: &&harness_contract::execution_graph::ExecutionNodeProjection| {
        node.work.as_ref().is_none_or(|work| work.required)
    };
    if projection
        .nodes
        .iter()
        .filter(required)
        .all(|node| node.status == ExecutionNodeStatus::Completed)
        && projection.nodes.iter().all(|node| {
            node.work.as_ref().is_none_or(|work| work.required) || node.status.is_terminal()
        })
    {
        "completed"
    } else if projection
        .nodes
        .iter()
        .filter(required)
        .any(|node| node.status == ExecutionNodeStatus::Failed)
    {
        "failed"
    } else if projection
        .nodes
        .iter()
        .filter(required)
        .any(|node| node.status == ExecutionNodeStatus::Blocked)
    {
        "blocked"
    } else {
        "running"
    }
}

fn bind_strategy(
    request: &mut RuntimeOrchestrationCommand,
    leased_decision: Option<&RuntimeExecutionDecision>,
    parent: Option<&ExecutionParentBinding>,
) {
    let team_requested = request.proposal.as_ref().is_some_and(|proposal| {
        proposal
            .nodes
            .iter()
            .any(|node| node.recipe == CapabilityRecipeId::Team)
    });
    if !team_requested {
        return;
    }
    // A root collaboration decision is already a Runtime-bound semantic
    // Program, so its concrete Team roles are Runtime-owned. Preserve the
    // explicit mode assigned at the narrow control boundary instead of
    // overwriting it with the legacy model-assisted role-selection mode.
    request
        .selection_mode
        .get_or_insert(harness_contract::team::TeamSelectionMode::ModelAssisted);
    if request.strategy_binding.is_none() {
        if let Some(decision) = leased_decision {
            request.strategy_binding = Some(harness_contract::team::TeamStrategyBinding {
                decision_id: decision.decision_id.clone(),
                decision_revision: decision.decision_revision,
                decision_lease: decision.lease.lease_id.clone(),
                turn_ref: decision
                    .turn_ref
                    .clone()
                    .or_else(|| parent.map(|binding| binding.execution_id.clone()))
                    .unwrap_or_else(|| "detached-orchestration".to_string()),
            });
        }
    }
}

async fn submit_approval(
    request: &mut RuntimeOrchestrationCommand,
    services: &RuntimeServices,
) -> Result<(), String> {
    let session_id = request.session_id.clone();
    let identity = format!(
        "{}\n{}\n{}\n{}",
        services.workspace_key(),
        session_id.as_deref().unwrap_or_default(),
        request.intent.trim(),
        request.operation.as_str(),
    );
    let digest = model_protocol::fingerprint::stable_hash_bytes(identity.as_bytes());
    let approval_id = format!("runtime-orchestration-{digest:016x}");
    let source = ApprovalSource {
        kind: ApprovalSourceKind::Session,
        session_id,
        agent_id: None,
        team_id: None,
        mission_id: None,
        resource_ref: None,
        review_ref: None,
        application: None,
    };
    let action = format!("runtime_orchestrate:{}", request.operation.as_str());
    let user_directed_custom_team = request.template_proposal.is_some()
        && request.proposal.as_ref().is_some_and(|proposal| {
            proposal
                .nodes
                .iter()
                .any(|node| node.recipe == CapabilityRecipeId::Team)
        });
    let timeout_policy = user_directed_custom_team
        .then_some(ApprovalTimeoutPolicy::AutoApproveOnce)
        .unwrap_or(ApprovalTimeoutPolicy::Pending);
    let veto_window_ms = services
        .execution_capacity_profile()
        .user_team_veto_window_ms;
    let expires_at_ms = user_directed_custom_team
        .then(|| crate::tool_invocation::now_ms().saturating_add(veto_window_ms));
    services.approval_queue().submit_scoped_with_deadline(
        approval_id.clone(),
        SubmitGlobalApprovalRequest {
            context: services.bind_session_policy_to_approval_context(
                harness_contract::policy::ApprovalContext::owned(
                    &source,
                    &action,
                    services.workspace_key(),
                ),
            ),
            source,
            action,
            summary: request.intent.chars().take(512).collect(),
            risk: request
                .constraints
                .risk
                .as_deref()
                .and_then(|risk| serde_json::from_str::<TaskRisk>(&format!("\"{risk}\"")).ok())
                .unwrap_or(TaskRisk::Critical),
            domain: harness_contract::policy::ApprovalDomain::Execution,
            blocks_execution: true,
            evidence_refs: request.evidence_refs.iter().take(64).cloned().collect(),
            timeout_policy,
        },
        expires_at_ms,
    )?;
    // The global approval router is the single authority. Autonomous and YOLO
    // auto-approve with an audit trail; lower levels queue for humans.
    let session_policy = request
        .session_id
        .as_deref()
        .and_then(|session_id| services.session_execution_policy(session_id));
    let profile = session_policy
        .as_ref()
        .map(|policy| policy.autonomy_profile)
        .unwrap_or(harness_contract::policy::AutonomyProfileId::Cautious);
    let decision = crate::approval_router::ApprovalRouter::resolve(
        profile,
        harness_contract::policy::ApprovalDomain::Execution,
        request
            .constraints
            .risk
            .as_deref()
            .and_then(|risk| serde_json::from_str::<TaskRisk>(&format!("\"{risk}\"")).ok())
            .unwrap_or(TaskRisk::Critical),
        true,
        false,
    );
    if matches!(
        decision,
        crate::approval_router::ApprovalDecision::AutoApprove
            | crate::approval_router::ApprovalDecision::StewardApprove
    ) {
        services
            .approval_queue()
            .decide_internal(ApprovalDecisionCommand {
                approval_id: approval_id.clone(),
                approved: true,
                skip: false,
                reason: match decision {
                    crate::approval_router::ApprovalDecision::AutoApprove => {
                        "session policy auto-approves Runtime orchestration".to_string()
                    }
                    crate::approval_router::ApprovalDecision::StewardApprove => {
                        "bounded Steward policy approved Runtime orchestration".to_string()
                    }
                    _ => unreachable!("non-approval decision reached approval branch"),
                },
                scope: ApprovalGrantScope::Once,
                actor: ApprovalDecisionActor {
                    kind: if decision == crate::approval_router::ApprovalDecision::StewardApprove {
                        ApprovalDecisionActorKind::StewardAgent
                    } else {
                        ApprovalDecisionActorKind::Policy
                    },
                    actor_id: if decision
                        == crate::approval_router::ApprovalDecision::StewardApprove
                    {
                        "runtime-approval-steward".to_string()
                    } else {
                        "session-policy".to_string()
                    },
                },
                evidence_refs: vec![
                    "approval.policy.auto_grant".to_string(),
                    format!("approval.router:{decision:?}"),
                ],
            })?;
    } else if user_directed_custom_team
        && decision == crate::approval_router::ApprovalDecision::Human
    {
        // A confirmation profile reserves a real veto interval. The existing
        // ApprovalCoordinator owns the durable wait and is woken by either a
        // human decision or the queue's single deadline worker; no admission
        // task polls queue state or holds a graph/resource lock while waiting.
        services
            .approval_coordinator()
            .wait_for_existing_execution(
                &approval_id,
                crate::CancellationToken::new(),
                None,
                std::time::Duration::from_millis(veto_window_ms),
            )
            .await?;
    }
    request.constraints.approval_id = Some(approval_id);
    Ok(())
}

fn mutation_applied(graph: &ExecutionGraph, mutation_id: &str) -> bool {
    graph.orchestration.as_ref().is_some_and(|metadata| {
        metadata.mutation_id == mutation_id
            || metadata
                .applied_mutation_ids
                .iter()
                .any(|applied| applied == mutation_id)
    })
}

fn mutation_id(request: &RuntimeOrchestrationCommand) -> Result<&str, String> {
    request
        .proposal
        .as_ref()
        .map(|proposal| proposal.mutation_id.as_str())
        .ok_or_else(|| "missing_mutation_id".to_string())
}

fn recipe_catalog() -> Vec<String> {
    [
        CapabilityRecipeId::Direct,
        CapabilityRecipeId::Agent,
        CapabilityRecipeId::Team,
        CapabilityRecipeId::Review,
        CapabilityRecipeId::Synthesis,
        CapabilityRecipeId::SessionDispatch,
    ]
    .into_iter()
    .map(|recipe| recipe.as_str().to_string())
    .collect()
}

fn request_id(request: &RuntimeOrchestrationCommand) -> String {
    request.proposal.as_ref().map_or_else(
        || format!("runtime-orch-{}", uuid::Uuid::new_v4()),
        |proposal| format!("runtime-orch-{}", proposal.mutation_id),
    )
}

#[cfg(test)]
fn session_is_trust_all(services: &RuntimeServices, session_id: Option<&str>) -> bool {
    let trust_all = session_id.is_some_and(|session_id| {
        services
            .session_execution_policy(session_id)
            .is_some_and(|policy| {
                policy.approval_profile == harness_contract::policy::ApprovalProfile::TrustAll
            })
    });
    if !trust_all {
        tracing::warn!(
            session_id = ?session_id,
            policy = ?session_id.and_then(|session_id| services.session_execution_policy(session_id)),
            "session_is_trust_all: trust-all policy not visible to orchestration"
        );
    }
    trust_all
}

fn result_from_outcome(
    request_id: &str,
    mut decision: RuntimeOrchestrationDecision,
    outcome: OperationOutcome,
) -> RuntimeOrchestrationResult {
    if let Some(findings) = outcome
        .execution
        .get("completion_findings")
        .and_then(Value::as_array)
    {
        decision.validation_findings.extend(
            findings
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string),
        );
        decision.validation_findings.sort();
        decision.validation_findings.dedup();
    }
    // Once a Program exists, its graph owns retry and recovery. A terminal
    // failure is not a model-repairable admission diagnostic: telling the
    // same root turn to choose a fresh decision id would create a second set
    // of Team side effects beside the immutable failed Program.
    if outcome.status == "blocked"
        && outcome
            .execution
            .get("collaboration_diagnostics")
            .and_then(Value::as_array)
            .is_some_and(|diagnostics| !diagnostics.is_empty())
    {
        let mut failure_kinds = outcome
            .execution
            .get("collaboration_diagnostics")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|diagnostic| diagnostic.get("failure_kind").and_then(Value::as_str))
            .collect::<Vec<_>>();
        failure_kinds.sort();
        failure_kinds.dedup();
        decision.recovery_hints.push(RecoveryHint {
            code: "collaboration_terminal_program_failed".to_string(),
            message: format!(
                "The admitted Program reached a terminal execution failure ({}). Its ExecutionGraph remains the sole retry/recovery authority; do not submit a replacement Program in this turn.",
                if failure_kinds.is_empty() {
                    "unspecified".to_string()
                } else {
                    failure_kinds.join(", ")
                }
            ),
            retryable: false,
        });
    }
    let mut evidence = outcome.evidence;
    if let Some(lease_id) = decision
        .budget
        .get("strategy_lease_id")
        .and_then(Value::as_str)
    {
        evidence["strategy_lease_id"] = Value::String(lease_id.to_string());
    }
    RuntimeOrchestrationResult {
        request_id: request_id.to_string(),
        status: outcome.status,
        decision,
        execution: outcome.execution,
        evidence,
        next_model_guidance: outcome.guidance,
    }
}

fn result_without_runtime(
    request: &RuntimeOrchestrationCommand,
    decision: RuntimeOrchestrationDecision,
) -> RuntimeOrchestrationResult {
    result_without_runtime_with_id(&request_id(request), request, decision)
}

fn result_without_runtime_with_id(
    request_id: &str,
    request: &RuntimeOrchestrationCommand,
    decision: RuntimeOrchestrationDecision,
) -> RuntimeOrchestrationResult {
    let status = decision.status.clone();
    RuntimeOrchestrationResult {
        request_id: request_id.to_string(),
        status: status.clone(),
        decision,
        execution: json!({"type":"orchestration_not_submitted","status":status}),
        evidence: json!({
            "accepted": false,
            "executed": false,
            "operation": request.operation.as_str(),
        }),
        next_model_guidance: if status == "needs_approval" {
            "Wait for the bound durable approval, then retry once with its approval_id.".to_string()
        } else {
            "Correct the semantic proposal from the validation findings; do not retry unchanged."
                .to_string()
        },
    }
}

fn unavailable_result(
    request: &RuntimeOrchestrationCommand,
    finding: String,
) -> RuntimeOrchestrationResult {
    let plan = planner::plan_runtime_orchestration(request);
    let mut decision = validator::validate_request(
        request,
        &plan.execution_decision,
        plan.model_proposal.as_ref(),
        None,
    );
    decision.status = "unavailable".to_string();
    decision.validation_findings.push(finding);
    result_without_runtime(request, decision)
}

#[must_use]
pub fn runtime_orchestration_response(value: Value) -> Value {
    runtime_orchestration_response_with_decision(value, None)
}

#[must_use]
pub fn runtime_orchestration_response_with_decision(
    value: Value,
    leased_decision: Option<&RuntimeExecutionDecision>,
) -> Value {
    match serde_json::from_value::<harness_contract::orchestration::ModelRuntimeOrchestrationInput>(
        value,
    ) {
        Ok(input) => {
            let request = RuntimeOrchestrationCommand::from_model(
                input,
                RuntimeOrchestrationBinding {
                    model_lease: None,
                    session_id: None,
                    lineage: None,
                    mission_id: None,
                    selection_mode: None,
                    strategy_binding: None,
                    capabilities: Vec::new(),
                    surface: None,
                    permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
                },
            );
            serde_json::to_value(handle_runtime_orchestration_request_with_decision(
                request,
                leased_decision,
            ))
            .unwrap_or_else(|error| json!({"status":"rejected","error":error.to_string()}))
        }
        Err(error) => {
            json!({"status":"rejected","error":format!("invalid runtime_orchestrate input: {error}")})
        }
    }
}
