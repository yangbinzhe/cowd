//! Model-visible semantic orchestration over Runtime-owned execution graphs.
//!
//! The model may inspect state and propose semantic topology. Runtime alone
//! resolves definitions, executors, leases, physical identities and commands.

pub mod collaboration_continuation;
pub(crate) mod collaboration_coordinator;
pub mod compiler;
pub(crate) mod input_disposition;
pub mod planner;
pub mod request;
pub mod result;
pub(crate) mod team_authority;
pub mod validator;

use std::collections::{BTreeMap, BTreeSet};

use crate::execution_core::graph::{ExecutionCommitError, ExecutionRunnerError};
use crate::execution_core::{
    graph::ExecutionRunReport, ExecutionStateStoreError, RuntimeExecutionDecision,
};
use crate::{
    ApprovalSource, ApprovalSourceKind, ApprovalTimeoutPolicy, ExecutionGraphHost, RuntimeServices,
    SubmitGlobalApprovalRequest,
};
use harness_contract::core::TaskRisk;
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphCommand, ExecutionGraphProjection, ExecutionNodeStatus,
    ExecutionParentBinding, ExecutionUsage,
};
use harness_contract::policy::{
    ApprovalContext, ApprovalDecisionActor, ApprovalDecisionActorKind, ApprovalDecisionCommand,
    ApprovalDomain, ApprovalGrantScope,
};
use serde_json::{json, Value};

const MAX_REVISION_CAS_ATTEMPTS: usize = 3;
const EPHEMERAL_TEMPLATE_TTL_MS: u64 = 60 * 60 * 1000;

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

/// Apply a typed AddTeam patch through the same bounded Coordinator submit
/// path as every other semantic revision. Callers cannot provide graph nodes,
/// executors or mutable Team identities.
pub async fn submit_collaboration_add_team_patch(
    graph_id: &str,
    patch: &harness_contract::execution_graph::CollaborationIntentPatch,
    services: &RuntimeServices,
) -> Result<RuntimeOrchestrationResult, String> {
    let graph = services
        .graph_state_store()
        .load_async(graph_id)
        .await
        .map_err(|error| format!("patch_target_load_failed:{error}"))?;
    let request = collaboration_coordinator::compile_add_team_patch(&graph, patch)?;
    Ok(submit_runtime_orchestration_request_controlled(
        request,
        None,
        services,
        graph.parent_execution,
        None,
    )
    .await)
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
    }
    submit_collaboration_add_team_patch(graph_id, &patch, services).await
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
    bind_strategy(&mut request, leased_decision, parent_execution.as_ref());
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
    let trust_all_session = session_is_trust_all(services, request.session_id.as_deref());
    let requires_orchestration_approval =
        request.constraints.risk.as_deref() == Some("critical") || trust_all_session;
    if requires_orchestration_approval && request.constraints.approval_id.is_none() {
        if let Err(error) = submit_approval(&mut request, services) {
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

/// Materialize the narrow model-facing custom-Team path.  Model JSON provides
/// only semantic topology and template content; Runtime alone creates the
/// immutable snapshot and binds it to the authenticated session/turn lineage.
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
    let [team_node] = team_nodes.as_slice() else {
        return Err("ephemeral_template_requires_exactly_one_team_node".to_string());
    };
    if team_node.template.is_some() {
        return Err("ephemeral_template_rejects_catalog_template_selector".to_string());
    }
    let policy_ref = request
        .session_id
        .as_deref()
        .and_then(|session_id| {
            services
                .session_execution_policy(session_id)
                .map(|policy| format!("session:{session_id}:policy:{}", policy.revision))
        })
        .unwrap_or_else(|| format!("session:{}:unversioned", lineage.session_id));
    let snapshot = compile_ephemeral_team_template_snapshot(
        template_proposal,
        lineage,
        request.constraints.permission_ceiling,
        policy_ref,
        crate::tool_invocation::now_ms().saturating_add(EPHEMERAL_TEMPLATE_TTL_MS),
        services,
    )?;
    request
        .ephemeral_team_templates
        .insert(team_node.node_id.clone(), snapshot);
    Ok(())
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
        match compiler::compile_orchestration(
            request_id,
            &attempt,
            &effective_plan,
            parent_execution.as_ref().cloned(),
            Some(services.team_runtime().as_ref()),
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
    if error.contains("Team acceptance criterion")
        && error.contains("has no bounded Runtime resource scope")
    {
        // The session does not grant bounded workspace write authority for
        // this proposal. Narrow every Team node to read-only research instead
        // of failing the whole orchestration; Runtime never widens authority,
        // and the terminal report can state that the workspace write was not
        // authorized. Write-acceptance criteria are removed with it, so a
        // missing artifact can never be silently reported as created.
        let mut repaired = false;
        if let Some(proposal) = request.proposal.as_mut() {
            for node in &mut proposal.nodes {
                if node.recipe != CapabilityRecipeId::Team {
                    continue;
                }
                node.template = Some("cowd/parallel-research-synthesis".to_string());
                node.output_artifacts = vec!["terminal_synthesis".to_string()];
                node.evidence_contract = vec![
                    "summary".to_string(),
                    "evidence".to_string(),
                    "unresolved".to_string(),
                ];
                // Stale role plans (e.g. `implementer`) belong to the old
                // write template; clearing focuses lets the compiler derive
                // fresh researcher/synthesizer plans for the read-only
                // template instead of failing role resolution.
                node.focuses = Vec::new();
                node.required = true;
                repaired = true;
            }
            if repaired {
                proposal
                    .completion
                    .required_artifact_kinds
                    .retain(|artifact| artifact != "workspace_change");
                request.constraints.requires_write = Some(false);
                tracing::warn!(
                    "orchestration repair: Team proposal downgraded to read-only research because no bounded workspace write scope exists"
                );
            }
        }
        return repaired;
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
        // topology.  The prefix is private to `compile_add_team_patch`; model
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
        let mut revision_repairs = Vec::new();
        let mut mutation = compiler::compile_graph_mutation(
            request_id,
            request,
            plan,
            proposal,
            graph_id,
            graph.parent_execution.as_ref(),
            services.team_runtime().as_ref(),
            &existing_semantic_node_instances,
            &mut revision_repairs,
        )
        .map_err(|error| format!("semantic_revision_compile_failed:{error}"))?;
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
                                        input_contract: Default::default(),
                                        state: Default::default(),
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
    let completed_team_count = projection
        .nodes
        .iter()
        .filter(|node| {
            node.executor_kind == compiler::TEAM_SUBGRAPH_EXECUTOR
                && node.status == ExecutionNodeStatus::Completed
        })
        .count()
        .max(1);
    let per_team_summary_chars = (12_000 / completed_team_count).clamp(512, 6_000);
    for node in projection.nodes.iter().filter(|node| {
        node.executor_kind == compiler::TEAM_SUBGRAPH_EXECUTOR
            && node.status == ExecutionNodeStatus::Completed
    }) {
        assessment.has_teams = true;
        let request = match serde_json::from_str::<harness_contract::team::TeamInstantiationRequest>(
            &node.payload_ref,
        ) {
            Ok(request) => request,
            Err(error) => {
                assessment.working_state_verified = false;
                assessment.findings.push(format!(
                    "team_subgraph_payload_invalid:{}:{error}",
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
            let terminal_summary = node.summary.as_deref().map(|summary| {
                summary
                    .chars()
                    .take(per_team_summary_chars)
                    .collect::<String>()
            });
            assessment.team_terminals.push(json!({
                "team_id": team_id,
                "graph_id": child_graph_id,
                "working_state_verified": materialization.is_ok(),
                "terminal_summary": terminal_summary,
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
    let mut findings = Vec::new();
    for required in &metadata.completion.required_node_ids {
        if projection
            .nodes
            .iter()
            .find(|node| node.node_id == *required)
            .is_none_or(|node| node.status != ExecutionNodeStatus::Completed)
        {
            findings.push(format!("required_node_not_completed:{required}"));
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
    request.selection_mode = Some(harness_contract::team::TeamSelectionMode::ModelAssisted);
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

fn submit_approval(
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
    services.approval_queue().submit_scoped(
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
            timeout_policy: ApprovalTimeoutPolicy::Pending,
        },
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

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::execution_graph::ExecutionCompletionContract;
    use harness_contract::policy::PermissionMode;

    #[test]
    fn trust_all_sessions_are_detected_for_orchestration_approval() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        assert!(!session_is_trust_all(&services, Some("session-1")));
        services.publish_session_execution_policy(
            "session-1",
            crate::permissions::SessionExecutionPolicyControl::from_policy(
                harness_contract::policy::SessionExecutionPolicy::from_profile(
                    harness_contract::policy::AutonomyProfileId::Yolo,
                    2,
                    harness_contract::policy::SessionExecutionPolicyOrigin::SessionExplicit,
                ),
            ),
        );
        assert!(session_is_trust_all(&services, Some("session-1")));
        assert!(!session_is_trust_all(&services, None));
    }

    #[test]
    fn semantic_revision_retries_only_typed_stale_errors_and_at_most_three_attempts() {
        let stale = ExecutionRunnerError::Commit(ExecutionCommitError::StaleRevision {
            graph_id: "graph-r4".to_string(),
            expected: 1,
            actual: 2,
        });
        let store_stale = ExecutionRunnerError::Commit(ExecutionCommitError::EventStore(
            crate::RuntimeEventStoreError::StaleRevision {
                stream_id: "graph:graph-r4".to_string(),
                expected: 1,
                actual: 2,
            },
        ));
        let non_stale = ExecutionRunnerError::Commit(ExecutionCommitError::InvalidReplan(
            "node collision".to_string(),
        ));

        assert!(semantic_revision_may_retry(&stale, 1));
        assert!(semantic_revision_may_retry(&store_stale, 2));
        assert!(!semantic_revision_may_retry(&stale, 3));
        assert!(!semantic_revision_may_retry(&non_stale, 1));
    }

    #[test]
    fn committed_change_paths_accept_only_typed_runtime_receipts() {
        let change = harness_contract::agent::AgentChangeReceipt {
            path: "reports/final.html".to_string(),
            before_sha256: None,
            after_sha256: "sha256:after".to_string(),
            write_sequence: 7,
        };
        let valid = harness_contract::context::EvidenceAccessRef::unavailable(
            harness_contract::context::EvidenceRef::observed(
                "runtime_change",
                serde_json::to_string(&change).expect("change receipt"),
            ),
            "application/vnd.cowd.runtime-change+json",
            "execution-node:writer",
        );
        let invalid = harness_contract::context::EvidenceAccessRef::unavailable(
            harness_contract::context::EvidenceRef::observed("runtime_change", "not-json"),
            "application/vnd.cowd.runtime-change+json",
            "execution-node:writer",
        );

        let (paths, invalid_receipts) = committed_change_paths(&[valid, invalid]);

        assert_eq!(paths, BTreeSet::from(["reports/final.html".to_string()]));
        assert_eq!(invalid_receipts, vec!["not-json".to_string()]);
    }

    #[test]
    fn write_scope_repair_downgrades_team_nodes_to_read_only_research() {
        let mut request = proposal(vec![GraphSemanticNode {
            node_id: "team-1".to_string(),
            recipe: CapabilityRecipeId::Team,
            objective: "review the repository".to_string(),
            depends_on: Vec::new(),
            multiplicity: 1,
            focuses: Vec::new(),
            template: Some("cowd/execute-review".to_string()),
            target_session_id: None,
            output_artifacts: vec![
                "workspace_change".to_string(),
                "terminal_synthesis".to_string(),
            ],
            evidence_contract: vec![
                "implementation".to_string(),
                "source_verification".to_string(),
                "evidence".to_string(),
                "risks".to_string(),
            ],
            required_evidence_refs: Vec::new(),
            resource_scopes: vec!["session:session-v621".to_string()],
            required: true,
            dependency: Default::default(),
            cancellation_group: None,
        }]);
        let error = "Team template resolution failed: Team acceptance criterion `implementation` has no bounded Runtime resource scope";
        assert!(repair_semantic_compilation(
            &mut request,
            std::path::Path::new("/tmp"),
            error
        ));
        let node = &request.proposal.as_ref().unwrap().nodes[0];
        assert_eq!(
            node.template.as_deref(),
            Some("cowd/parallel-research-synthesis")
        );
        assert_eq!(
            node.output_artifacts,
            vec!["terminal_synthesis".to_string()]
        );
        assert_eq!(
            node.evidence_contract,
            vec![
                "summary".to_string(),
                "evidence".to_string(),
                "unresolved".to_string(),
            ]
        );
        assert!(node.focuses.is_empty());
        assert!(!request
            .proposal
            .as_ref()
            .unwrap()
            .completion
            .required_artifact_kinds
            .iter()
            .any(|artifact| artifact == "workspace_change"));
        assert_eq!(request.constraints.requires_write, Some(false));
    }

    fn proposal(nodes: Vec<GraphSemanticNode>) -> RuntimeOrchestrationCommand {
        RuntimeOrchestrationCommand {
            intent: "analyze independent domains and synthesize checked evidence".to_string(),
            model_lease: Some("test-model".to_string()),
            session_id: Some("session-v621".to_string()),
            lineage: Some(harness_contract::execution_graph::ExecutionGraphLineage {
                session_id: "session-v621".to_string(),
                turn_id: "turn-v621".to_string(),
                root_task_id: "task-root-v621".to_string(),
                task_id: "task-root-v621".to_string(),
                generation: 1,
            }),
            mission_id: Some("mission-v621".to_string()),
            operation: RuntimeOrchestrationOperation::Propose,
            inspect_execution_id: None,
            proposal: Some(GraphMutationProposal {
                mutation_id: "mutation-v621".to_string(),
                target_execution_id: None,
                expected_revision: None,
                nodes,
                completion: Default::default(),
                collaboration_program: None,
                reason: "parallel evidence lanes".to_string(),
            }),
            control: None,
            template_proposal: None,
            ephemeral_team_templates: Default::default(),

            input_disposition: None,
            selection_mode: None,
            strategy_binding: None,
            capabilities: Vec::new(),
            evidence_refs: Vec::new(),
            constraints: RuntimeOrchestrationConstraints {
                max_parallel_agents: Some(8),
                permission_ceiling: PermissionMode::ReadOnly,
                ..Default::default()
            },
            surface: Some("test".to_string()),
        }
    }

    fn node(id: &str, recipe: CapabilityRecipeId, depends_on: Vec<String>) -> GraphSemanticNode {
        GraphSemanticNode {
            node_id: id.to_string(),
            recipe,
            objective: format!("complete {id}"),
            depends_on,
            multiplicity: 1,
            focuses: Vec::new(),
            template: None,
            target_session_id: None,
            output_artifacts: Vec::new(),
            evidence_contract: Vec::new(),
            required_evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            required: true,
            dependency: Default::default(),
            cancellation_group: None,
        }
    }

    fn ensure_test_team_resource(request: &mut RuntimeOrchestrationCommand) {
        let Some(proposal) = request.proposal.as_mut() else {
            return;
        };
        if proposal
            .nodes
            .iter()
            .filter(|node| node.recipe == CapabilityRecipeId::Team)
            .all(|node| node.resource_scopes.is_empty())
        {
            request.capabilities.push("resource:network:*".to_string());
            for node in proposal
                .nodes
                .iter_mut()
                .filter(|node| node.recipe == CapabilityRecipeId::Team)
            {
                node.resource_scopes.push("network:*".to_string());
            }
        }
    }

    fn ensure_test_mission(services: &RuntimeServices) {
        services
            .mission_runtime()
            .create_mission(
                "mission-v621",
                "test semantic orchestration",
                vec![harness_contract::reality::EvidenceRef::observed(
                    "test",
                    "mission-v621",
                )],
            )
            .expect("test Mission");
    }

    #[test]
    fn propose_with_custom_template_materializes_a_turn_bound_team_snapshot() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        ensure_test_mission(&services);
        let mut team = node(
            "independent-assessment",
            CapabilityRecipeId::Team,
            Vec::new(),
        );
        team.objective = "independently assess the bounded evidence".to_string();
        team.output_artifacts = vec!["assessment".to_string()];
        team.evidence_contract = vec!["summary".to_string(), "evidence".to_string()];
        let mut request = proposal(vec![team]);
        request.template_proposal = Some(json!({
            "template_id": "cowd/turn-scoped-independent-assessment",
            "name": "Turn scoped independent assessment",
            "team_display_name": "独立评估",
            "roles": [{
                "role_id": "evidence_assessor",
                "display_name": "证据评估师",
                "responsibility": "独立检查已授权证据并报告不确定性",
                "agent_definition_ref": "workspace/cowd/nonexistent@1",
                "grant_ceiling": ["read"],
                "fixed_count": 1,
                "acceptance": ["summary", "evidence"],
                "behavior": [{"kind": "reacquire_evidence", "required": true}]
            }],
            "result_fields": ["summary", "evidence"],
            "evidence_required": true,
            "instructions": "# 独立评估\n\n只使用已授权证据，清楚列出不确定性。"
        }));

        materialize_ephemeral_team_template(&mut request, &services)
            .expect("normal propose ingress materializes the snapshot");
        assert!(request.template_proposal.is_none());
        let snapshot = request
            .ephemeral_team_templates
            .get("independent-assessment")
            .expect("snapshot is owned by the Team node");
        assert_eq!(snapshot.session_id, "session-v621");
        assert_eq!(snapshot.turn_id, "turn-v621");
        assert!(services
            .definition_registry()
            .resolve_team(
                &snapshot.revision.revision_ref.template_id,
                harness_contract::agent::RevisionSelector::LatestApprovedStable,
            )
            .is_err());

        team_authority::bind_semantic_resource_authority(
            &mut request,
            None,
            services.workspace_root(),
        );
        ensure_test_team_resource(&mut request);
        let plan = planner::plan_runtime_orchestration(&request);
        let compiled = compiler::compile_orchestration(
            "turn-scoped-custom-team",
            &request,
            &plan,
            None,
            Some(services.team_runtime().as_ref()),
        )
        .expect("the normal propose compiler uses the snapshot");
        let child_request: harness_contract::team::TeamInstantiationRequest =
            serde_json::from_str(&compiled.graph.nodes[0].payload_ref)
                .expect("typed Team child request");
        assert!(matches!(
            child_request.template_selector,
            harness_contract::team::TeamTemplateSelector::Ephemeral { .. }
        ));
    }

    #[test]
    fn semantic_contract_rejects_physical_executor_injection() {
        let parsed = serde_json::from_value::<RuntimeOrchestrationCommand>(json!({
            "intent": "inject executor",
            "operation": "propose",
            "proposal": {
                "mutation_id": "bad",
                "reason": "bad",
                "nodes": [{
                    "node_id": "bad",
                    "recipe": "agent",
                    "objective": "bad",
                    "executor_kind": "shell"
                }]
            }
        }));
        assert!(parsed.is_err());
    }

    #[test]
    fn semantic_contract_cannot_deserialize_runtime_owned_ephemeral_snapshots() {
        let mut encoded = serde_json::to_value(proposal(vec![node(
            "team",
            CapabilityRecipeId::Team,
            Vec::new(),
        )]))
        .expect("serialize semantic request");
        encoded["ephemeral_team_templates"] = json!({"team": {"forged": true}});
        let parsed: RuntimeOrchestrationCommand =
            serde_json::from_value(encoded).expect("Runtime-owned field is ignored at boundary");
        assert!(parsed.ephemeral_team_templates.is_empty());
    }

    #[test]
    fn semantic_validator_rejects_dependency_cycle() {
        let request = proposal(vec![
            node("a", CapabilityRecipeId::Agent, vec!["b".to_string()]),
            node("b", CapabilityRecipeId::Review, vec!["a".to_string()]),
        ]);
        let plan = planner::plan_runtime_orchestration(&request);
        let decision = validator::validate_request(
            &request,
            &plan.execution_decision,
            plan.model_proposal.as_ref(),
            None,
        );
        assert_eq!(decision.status, "rejected");
        assert!(decision
            .validation_findings
            .contains(&"proposal_dependency_cycle".to_string()));
    }

    #[test]
    fn semantic_validator_limits_concurrent_wave_not_total_graph_work() {
        let mut research = node("research", CapabilityRecipeId::Agent, Vec::new());
        research.multiplicity = 3;
        let synthesis = node(
            "synthesis",
            CapabilityRecipeId::Synthesis,
            vec!["research".to_string()],
        );
        let review = node(
            "review",
            CapabilityRecipeId::Review,
            vec!["synthesis".to_string()],
        );
        let mut request = proposal(vec![research, synthesis, review]);
        request.constraints.max_parallel_agents = Some(3);
        let plan = planner::plan_runtime_orchestration(&request);
        let decision = validator::validate_request(
            &request,
            &plan.execution_decision,
            plan.model_proposal.as_ref(),
            None,
        );

        assert_ne!(decision.status, "rejected");
        assert!(!decision
            .validation_findings
            .contains(&"proposal_exceeds_parallel_agent_ceiling".to_string()));
    }

    #[test]
    fn semantic_validator_rejects_optional_effect_owner_before_materialization() {
        let mut team = node("team", CapabilityRecipeId::Team, Vec::new());
        team.required = false;
        let request = proposal(vec![team]);
        let plan = planner::plan_runtime_orchestration(&request);
        let decision = validator::validate_request(
            &request,
            &plan.execution_decision,
            plan.model_proposal.as_ref(),
            None,
        );
        assert_eq!(decision.status, "rejected");
        assert!(decision
            .validation_findings
            .contains(&"optional_semantic_node_owns_effect".to_string()));
    }

    #[test]
    fn semantic_compiler_materializes_parallel_agents_and_synthesis() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let mut agents = node("research", CapabilityRecipeId::Agent, Vec::new());
        agents.multiplicity = 2;
        agents.output_artifacts = vec!["research_finding".to_string()];
        let mut synthesis = node(
            "synthesis",
            CapabilityRecipeId::Synthesis,
            vec!["research".to_string()],
        );
        synthesis.output_artifacts = vec!["report".to_string()];
        let mut request = proposal(vec![agents, synthesis]);
        request.proposal.as_mut().unwrap().completion = ExecutionCompletionContract {
            required_node_ids: vec!["synthesis".to_string()],
            required_artifact_kinds: vec!["report".to_string()],
            allow_unresolved_conflicts: false,
        };
        team_authority::bind_semantic_resource_authority(
            &mut request,
            None,
            services.workspace_root(),
        );
        ensure_test_team_resource(&mut request);
        let plan = planner::plan_runtime_orchestration(&request);
        let compiled = compiler::compile_orchestration(
            "compile-v621",
            &request,
            &plan,
            None,
            Some(services.team_runtime().as_ref()),
        )
        .expect("semantic graph compiles");
        assert_eq!(compiled.graph.nodes.len(), 3);
        assert_eq!(compiled.graph.edges.len(), 4);
        assert_eq!(
            compiled
                .graph
                .edges
                .iter()
                .filter(|edge| {
                    edge.kind == harness_contract::execution_graph::ExecutionEdgeKind::Produces
                })
                .count(),
            2
        );
        let completion = &compiled.graph.orchestration.as_ref().unwrap().completion;
        assert_eq!(completion.required_node_ids.len(), 1);
        assert!(completion.required_node_ids[0].contains("synthesis"));
        let mut terminal = compiled.graph.clone();
        for node in &terminal.nodes {
            terminal.node_statuses.insert(
                node.id.clone(),
                if node.work.as_ref().is_some_and(|work| !work.required) {
                    ExecutionNodeStatus::Cancelled
                } else {
                    ExecutionNodeStatus::Completed
                },
            );
        }
        let projection = harness_contract::execution_graph::project_execution_graph(&terminal);
        assert_eq!(graph_status(&projection), "completed");
        assert_eq!(completion.required_artifact_kinds, vec!["report"]);
    }

    #[test]
    fn semantic_compiler_exposes_quorum_and_optional_lanes_to_the_runner() {
        use harness_contract::execution_graph::ExecutionDependencyPolicy;

        let services = RuntimeServices::in_memory().expect("runtime services");
        let mut left = node("left", CapabilityRecipeId::Agent, Vec::new());
        left.required = false;
        left.cancellation_group = Some("research".to_string());
        let mut right = node("right", CapabilityRecipeId::Review, Vec::new());
        right.required = false;
        right.cancellation_group = Some("research".to_string());
        let mut synthesis = node(
            "synthesis",
            CapabilityRecipeId::Synthesis,
            vec!["left".to_string(), "right".to_string()],
        );
        synthesis.dependency = ExecutionDependencyPolicy::Quorum {
            minimum: 1,
            cancel_remaining: true,
        };
        synthesis.cancellation_group = Some("research".to_string());
        let request = proposal(vec![left, right, synthesis]);
        let plan = planner::plan_runtime_orchestration(&request);
        let compiled = compiler::compile_orchestration(
            "quorum-v625",
            &request,
            &plan,
            None,
            Some(services.team_runtime().as_ref()),
        )
        .expect("quorum graph compiles");
        let optional = compiled
            .graph
            .nodes
            .iter()
            .filter(|node| node.work.as_ref().is_some_and(|work| !work.required))
            .count();
        assert_eq!(optional, 2);
        for node in compiled
            .graph
            .nodes
            .iter()
            .filter(|node| node.work.as_ref().is_some_and(|work| !work.required))
        {
            let intent: harness_contract::agent::AgentTaskIntent =
                serde_json::from_str(&node.payload_ref).expect("optional agent intent");
            assert_eq!(
                intent.permission_ceiling,
                harness_contract::policy::PermissionMode::ReadOnly
            );
            assert!(!intent
                .granted_capabilities
                .contains(&harness_contract::agent::AgentCapability::Write));
        }
        let synthesis = compiled
            .graph
            .nodes
            .iter()
            .find(|node| node.id.contains("synthesis"))
            .and_then(|node| node.work.as_ref())
            .expect("synthesis work contract");
        assert_eq!(
            synthesis.dependency,
            ExecutionDependencyPolicy::Quorum {
                minimum: 1,
                cancel_remaining: true,
            }
        );
        assert_eq!(synthesis.cancellation_group.as_deref(), Some("research"));
        let completion = &compiled
            .graph
            .orchestration
            .as_ref()
            .expect("orchestration")
            .completion;
        assert_eq!(completion.required_node_ids.len(), 1);
        assert!(completion.required_node_ids[0].contains("synthesis"));
        let mut terminal = compiled.graph.clone();
        for node in &terminal.nodes {
            terminal.node_statuses.insert(
                node.id.clone(),
                if node.work.as_ref().is_some_and(|work| !work.required) {
                    ExecutionNodeStatus::Cancelled
                } else {
                    ExecutionNodeStatus::Completed
                },
            );
        }
        let projection = harness_contract::execution_graph::project_execution_graph(&terminal);
        assert_eq!(graph_status(&projection), "completed");
        assert!(completion_findings(&projection).is_empty());
    }

    #[test]
    fn semantic_compiler_rejects_observed_negative_provider_lift() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let request = proposal(vec![
            node("left", CapabilityRecipeId::Agent, Vec::new()),
            node("right", CapabilityRecipeId::Review, Vec::new()),
        ]);
        let mut plan = planner::plan_runtime_orchestration(&request);
        let resources = &mut plan.execution_decision.strategy.resource_snapshot;
        resources.provider_effective_limit = 4;
        resources.provider_concurrency = 4;
        resources.tool_concurrency = 4;
        resources.team_slots = 4;
        resources.provider_queue_p95_ms = 300;
        resources.provider_service_p95_ms = 100;
        resources.sample_count = 4;
        let error = compiler::compile_orchestration(
            "provider-pressure-v625",
            &request,
            &plan,
            None,
            Some(services.team_runtime().as_ref()),
        )
        .expect_err("observed negative lift must reject fan-out");

        assert!(error
            .to_string()
            .contains("provider_queue_dominates_service_time"));
    }

    #[test]
    fn semantic_compiler_materializes_three_teams_and_a_review_team() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        ensure_test_mission(&services);
        let mut teams = ["domain-a", "domain-b", "domain-c"]
            .into_iter()
            .map(|id| {
                let mut team = node(id, CapabilityRecipeId::Team, Vec::new());
                team.template = Some("cowd/parallel-research-synthesis".to_string());
                team.evidence_contract = vec!["summary".to_string(), "evidence".to_string()];
                team.output_artifacts = vec![format!("{id}-finding")];
                team.evidence_contract = vec!["summary".to_string(), "evidence".to_string()];
                team
            })
            .collect::<Vec<_>>();
        let mut review = node(
            "review-team",
            CapabilityRecipeId::Team,
            vec![
                "domain-a".to_string(),
                "domain-b".to_string(),
                "domain-c".to_string(),
            ],
        );
        review.template = Some("cowd/parallel-research-synthesis".to_string());
        review.output_artifacts = vec!["reviewed-report".to_string()];
        review.evidence_contract = vec![
            "summary".to_string(),
            "evidence".to_string(),
            "unresolved".to_string(),
        ];
        teams.push(review);
        let mut request = proposal(teams);
        request.constraints.max_parallel_agents = Some(4);
        request.proposal.as_mut().unwrap().completion = ExecutionCompletionContract {
            required_node_ids: vec!["review-team".to_string()],
            required_artifact_kinds: vec!["reviewed-report".to_string()],
            allow_unresolved_conflicts: false,
        };
        team_authority::bind_semantic_resource_authority(
            &mut request,
            None,
            services.workspace_root(),
        );
        ensure_test_team_resource(&mut request);
        let plan = planner::plan_runtime_orchestration(&request);
        let compiled = compiler::compile_orchestration(
            "multi-team-v621",
            &request,
            &plan,
            None,
            Some(services.team_runtime().as_ref()),
        )
        .expect("multi-Team root compiles");
        assert_eq!(compiled.graph.nodes.len(), 4);
        assert!(compiled.graph.nodes.iter().all(|node| {
            node.kind == harness_contract::execution_graph::ExecutionNodeKind::Subgraph
                && node.executor_kind == compiler::TEAM_SUBGRAPH_EXECUTOR
        }));
        assert_eq!(
            compiled
                .graph
                .edges
                .iter()
                .filter(|edge| {
                    edge.kind == harness_contract::execution_graph::ExecutionEdgeKind::DependsOn
                })
                .count(),
            3
        );
        assert_eq!(
            compiled
                .graph
                .edges
                .iter()
                .filter(|edge| {
                    edge.kind == harness_contract::execution_graph::ExecutionEdgeKind::Produces
                })
                .count(),
            3
        );
        let completion = &compiled.graph.orchestration.as_ref().unwrap().completion;
        assert_eq!(completion.required_node_ids.len(), 1);
        assert!(completion.required_node_ids[0].contains("review-team"));
    }

    #[test]
    fn hundred_teams_remain_bounded_root_subgraphs() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        ensure_test_mission(&services);
        let nodes = (0..100)
            .map(|index| {
                let mut team = node(
                    &format!("team-{index:03}"),
                    CapabilityRecipeId::Team,
                    Vec::new(),
                );
                team.template = Some("cowd/parallel-research-synthesis".to_string());
                team.evidence_contract = vec!["summary".to_string(), "evidence".to_string()];
                team
            })
            .collect::<Vec<_>>();
        let mut request = proposal(nodes);
        request.constraints.max_parallel_agents = Some(100);
        team_authority::bind_semantic_resource_authority(
            &mut request,
            None,
            services.workspace_root(),
        );
        ensure_test_team_resource(&mut request);
        let plan = planner::plan_runtime_orchestration(&request);
        let compiled = compiler::compile_orchestration(
            "hundred-team-v621",
            &request,
            &plan,
            None,
            Some(services.team_runtime().as_ref()),
        )
        .expect("bounded root compiles");
        assert_eq!(compiled.graph.nodes.len(), 100);
        assert!(compiled.graph.nodes.iter().all(|node| {
            node.kind == harness_contract::execution_graph::ExecutionNodeKind::Subgraph
                && serde_json::from_str::<harness_contract::team::TeamInstantiationRequest>(
                    &node.payload_ref,
                )
                .is_ok()
        }));
        assert!(compiled.graph.nodes.iter().all(|node| {
            node.kind != harness_contract::execution_graph::ExecutionNodeKind::AgentTask
        }));
    }

    #[test]
    fn completion_contract_blocks_missing_artifacts_and_unresolved_conflicts() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let mut synthesis = node("synthesis", CapabilityRecipeId::Synthesis, Vec::new());
        synthesis.output_artifacts = vec!["verified-report".to_string()];
        let mut request = proposal(vec![synthesis]);
        request.proposal.as_mut().unwrap().completion = ExecutionCompletionContract {
            required_node_ids: vec!["synthesis".to_string()],
            required_artifact_kinds: vec!["verified-report".to_string()],
            allow_unresolved_conflicts: false,
        };
        team_authority::bind_semantic_resource_authority(
            &mut request,
            None,
            services.workspace_root(),
        );
        let plan = planner::plan_runtime_orchestration(&request);
        let mut graph = compiler::compile_orchestration(
            "completion-v621",
            &request,
            &plan,
            None,
            Some(services.team_runtime().as_ref()),
        )
        .expect("completion graph compiles")
        .graph;
        let node_id = graph.nodes[0].id.clone();
        graph.node_statuses.insert(
            node_id.clone(),
            harness_contract::execution_graph::ExecutionNodeStatus::Completed,
        );
        let projection = harness_contract::execution_graph::project_execution_graph(&graph);
        assert_eq!(
            completion_findings(&projection),
            vec!["required_artifact_not_materialized:verified-report"]
        );

        graph.node_results.insert(
            node_id,
            harness_contract::execution_graph::ExecutionNodeResult {
                status: harness_contract::execution_graph::ExecutionNodeStatus::Completed,
                result_ref: Some("artifact:verified-report:unresolved".to_string()),
                summary: Some("conflicting evidence retained".to_string()),
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            },
        );
        let findings = completion_findings(
            &harness_contract::execution_graph::project_execution_graph(&graph),
        );
        assert_eq!(findings, vec!["unresolved_conflict_rejected"]);
    }

    #[tokio::test]
    async fn team_board_is_revisioned_idempotent_and_binding_scoped() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        ensure_test_mission(&services);
        let mut team = node("team", CapabilityRecipeId::Team, Vec::new());
        team.template = Some("cowd/parallel-research-synthesis".to_string());
        let mut request = proposal(vec![team]);
        request.strategy_binding = Some(harness_contract::team::TeamStrategyBinding {
            decision_id: "decision-v621".to_string(),
            decision_revision: 1,
            decision_lease: "lease-v621".to_string(),
            turn_ref: "turn-v621".to_string(),
        });
        team_authority::bind_semantic_resource_authority(
            &mut request,
            None,
            services.workspace_root(),
        );
        if request.proposal.as_ref().unwrap().nodes[0]
            .resource_scopes
            .is_empty()
        {
            request.capabilities.push("resource:network:*".to_string());
            request.proposal.as_mut().unwrap().nodes[0]
                .resource_scopes
                .push("network:*".to_string());
        }
        let plan = planner::plan_runtime_orchestration(&request);
        let compiled = compiler::compile_orchestration(
            "team-board-v621",
            &request,
            &plan,
            None,
            Some(services.team_runtime().as_ref()),
        )
        .expect("team root compiles");
        let team_request: harness_contract::team::TeamInstantiationRequest =
            serde_json::from_str(&compiled.graph.nodes[0].payload_ref).expect("typed team request");
        let child = services
            .team_runtime()
            .plan(team_request)
            .expect("team child plan");
        let registered = services
            .execution_supervisor()
            .register_graph(child.graph)
            .await
            .expect("register team child");
        let agent_nodes = registered
            .nodes
            .iter()
            .filter(|node| {
                node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask
            })
            .map(|node| node.id.clone())
            .collect::<Vec<_>>();
        assert!(agent_nodes.len() >= 2);
        let publish = crate::TeamWorkingStatePublishRequest {
            graph_id: registered.id.clone(),
            node_id: agent_nodes[0].clone(),
            expected_revision: 0,
            kind: crate::TeamWorkingStateKind::Finding,
            summary: "checked semantic finding".to_string(),
            refs: vec!["evidence:test:v621".to_string()],
            artifact_refs: vec!["artifact:test:v621".to_string()],
            visibility: crate::TeamWorkingStateVisibility::Team,
        };
        let committed = services
            .team_runtime()
            .publish_working_state(publish.clone())
            .await
            .expect("publish board entry");
        assert_eq!(committed.board_revision, 1);
        let duplicate = services
            .team_runtime()
            .publish_working_state(publish)
            .await
            .expect("idempotent retry");
        assert_eq!(duplicate.entries.len(), 1);
        let visible = services
            .team_runtime()
            .read_working_state(crate::TeamWorkingStateReadRequest {
                graph_id: registered.id.clone(),
                node_id: agent_nodes[1].clone(),
                after_revision: Some(0),
                exact_revision: None,
            })
            .expect("peer read");
        assert_eq!(visible.entries.len(), 1);
        assert_eq!(visible.entries[0].source_generation, 1);
        let exact = services
            .team_runtime()
            .read_working_state(crate::TeamWorkingStateReadRequest {
                graph_id: registered.id.clone(),
                node_id: agent_nodes[1].clone(),
                after_revision: None,
                exact_revision: Some(1),
            })
            .expect("exact revision read");
        assert_eq!(exact.entries.len(), 1);
        let after = services
            .team_runtime()
            .read_working_state(crate::TeamWorkingStateReadRequest {
                graph_id: registered.id.clone(),
                node_id: agent_nodes[1].clone(),
                after_revision: Some(1),
                exact_revision: None,
            })
            .expect("read after committed revision");
        assert!(after.entries.is_empty());

        let private_reasoning = services
            .team_runtime()
            .publish_working_state(crate::TeamWorkingStatePublishRequest {
                graph_id: registered.id,
                node_id: agent_nodes[0].clone(),
                expected_revision: 1,
                kind: crate::TeamWorkingStateKind::Finding,
                summary: "raw chain-of-thought must remain private".to_string(),
                refs: Vec::new(),
                artifact_refs: Vec::new(),
                visibility: crate::TeamWorkingStateVisibility::Private,
            })
            .await
            .expect_err("private reasoning trace must be rejected");
        assert!(private_reasoning.contains("not private reasoning traces"));
    }

    #[tokio::test]
    async fn collaboration_coordinator_persists_every_compiled_team_obligation_before_admission() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        ensure_test_mission(&services);
        let mut first = node("research", CapabilityRecipeId::Team, Vec::new());
        first.template = Some("cowd/parallel-research-synthesis".to_string());
        let mut second = node("review", CapabilityRecipeId::Team, Vec::new());
        second.template = Some("cowd/parallel-research-synthesis".to_string());
        let mut request = proposal(vec![first, second]);
        request.strategy_binding = Some(harness_contract::team::TeamStrategyBinding {
            decision_id: "coordinator-obligations".to_string(),
            decision_revision: 1,
            decision_lease: "coordinator-lease".to_string(),
            turn_ref: "turn-v621".to_string(),
        });
        team_authority::bind_semantic_resource_authority(
            &mut request,
            None,
            services.workspace_root(),
        );
        ensure_test_team_resource(&mut request);
        let plan = planner::plan_runtime_orchestration(&request);
        let compiled = compiler::compile_orchestration(
            "coordinator-obligations",
            &request,
            &plan,
            None,
            Some(services.team_runtime().as_ref()),
        )
        .expect("team root compiles");
        let mut graph = services
            .compile_graph_agent_intents(compiled.graph)
            .expect("agent intents compile");
        collaboration_coordinator::prepare_program_admission(
            &mut graph,
            services.team_runtime().as_ref(),
        )
        .expect("program admission control compiles");
        let program = graph
            .orchestration
            .as_ref()
            .and_then(|metadata| metadata.collaboration_program.as_ref())
            .expect("Team graph has a Program");
        assert_eq!(
            program.control.lifecycle,
            harness_contract::execution_graph::CollaborationProgramLifecycle::Admitting
        );
        assert_eq!(
            program.control.obligations.len(),
            program.team_instances.len(),
            "every requested Team must be durable before the graph is registered"
        );
        assert!(program.control.obligations.iter().all(|obligation| {
            obligation.binding_ref.starts_with("team-binding:sha256:")
                && obligation.state
                    == harness_contract::execution_graph::TeamAdmissionState::Admitting
                && obligation.child_graph_ref.is_none()
        }));
        assert!(program.control.resource_ledger.context_reservation_tokens > 0);
        assert!(program.control.resource_ledger.output_reservation_tokens > 0);
        assert!(program.control.resource_ledger.parallel_demand >= 2);
        assert!(program.control.resource_ledger.deadline_at_ms > 0);
        program.validate().expect("active Program is complete");
        let root_node_ids = program
            .team_instances
            .iter()
            .map(|instance| {
                let (semantic, ordinal) = instance
                    .instance_id
                    .rsplit_once(':')
                    .expect("stable semantic instance id");
                program.semantic_node_instances[semantic][ordinal
                    .parse::<usize>()
                    .expect("stable instance ordinal")
                    .saturating_sub(1)]
                .clone()
            })
            .collect::<Vec<_>>();
        let registered = services
            .execution_supervisor()
            .register_graph(graph)
            .await
            .expect("register Program graph");
        for node_id in &root_node_ids {
            collaboration_coordinator::mark_team_admitted(
                &registered.id,
                node_id,
                &format!("team-graph:{node_id}"),
                services.execution_supervisor().as_ref(),
                services.graph_state_store(),
            )
            .await
            .expect("mark Team admitted");
        }
        collaboration_coordinator::mark_team_admitted(
            &registered.id,
            &root_node_ids[0],
            &format!("team-graph:{}", root_node_ids[0]),
            services.execution_supervisor().as_ref(),
            services.graph_state_store(),
        )
        .await
        .expect("duplicate admission is idempotent");
        let stored = services
            .graph_state_store()
            .load_async(&registered.id)
            .await
            .expect("load registered Program");
        let control = &stored
            .orchestration
            .as_ref()
            .expect("metadata")
            .collaboration_program
            .as_ref()
            .expect("Program")
            .control;
        assert_eq!(
            control.lifecycle,
            harness_contract::execution_graph::CollaborationProgramLifecycle::Running
        );
        assert!(control.obligations.iter().all(|obligation| {
            obligation.state == harness_contract::execution_graph::TeamAdmissionState::Admitted
                && obligation.child_graph_ref.is_some()
        }));
    }

    #[tokio::test]
    async fn startup_reconciliation_restores_live_program_approval_wait_state() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        ensure_test_mission(&services);
        let mut team = node("research", CapabilityRecipeId::Team, Vec::new());
        team.template = Some("cowd/parallel-research-synthesis".to_string());
        let mut request = proposal(vec![team]);
        team_authority::bind_semantic_resource_authority(
            &mut request,
            None,
            services.workspace_root(),
        );
        ensure_test_team_resource(&mut request);
        let plan = planner::plan_runtime_orchestration(&request);
        let mut graph = services
            .compile_graph_agent_intents(
                compiler::compile_orchestration(
                    "startup-program-wait",
                    &request,
                    &plan,
                    None,
                    Some(services.team_runtime().as_ref()),
                )
                .expect("Team Program compiles")
                .graph,
            )
            .expect("Agent intents compile");
        collaboration_coordinator::prepare_program_admission(
            &mut graph,
            services.team_runtime().as_ref(),
        )
        .expect("Program admission control compiles");
        let node_id = graph.nodes[0].id.clone();
        let graph = services
            .commit_service()
            .register_graph(graph)
            .expect("register Program")
            .graph;
        let graph = services
            .commit_service()
            .transition_node(
                &graph,
                &node_id,
                harness_contract::execution_graph::ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .expect("make Team root ready")
            .graph;
        let graph = services
            .commit_service()
            .transition_node(
                &graph,
                &node_id,
                harness_contract::execution_graph::ExecutionNodeStatus::Running,
                None,
                Vec::new(),
            )
            .expect("make Team root running")
            .graph;
        let graph = services
            .commit_service()
            .transition_node(
                &graph,
                &node_id,
                harness_contract::execution_graph::ExecutionNodeStatus::WaitingApproval,
                None,
                Vec::new(),
            )
            .expect("persist approval wait")
            .graph;

        let examined = collaboration_coordinator::reconcile_terminal_programs_on_startup(
            services.execution_supervisor().as_ref(),
            services.graph_state_store(),
            16,
        )
        .await
        .expect("startup reconciliation");
        assert_eq!(examined, 1);
        let stored = services
            .graph_state_store()
            .load_async(&graph.id)
            .await
            .expect("load reconciled Program");
        let control = &stored
            .orchestration
            .as_ref()
            .and_then(|metadata| metadata.collaboration_program.as_ref())
            .expect("Program")
            .control;
        assert_eq!(
            control.lifecycle,
            harness_contract::execution_graph::CollaborationProgramLifecycle::AwaitingApproval
        );
        assert_eq!(
            control.blocker_ref.as_deref(),
            Some(format!("execution-node:{node_id}").as_str())
        );
    }

    #[test]
    fn add_team_patch_compiles_to_an_exact_active_program_revision() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        ensure_test_mission(&services);
        let mut seed_team = node("research", CapabilityRecipeId::Team, Vec::new());
        seed_team.objective = "collect the bounded research evidence".to_string();
        seed_team.output_artifacts = vec!["research".to_string()];
        seed_team.evidence_contract = vec!["summary".to_string(), "evidence".to_string()];
        let mut seed_request = proposal(vec![seed_team]);
        seed_request.template_proposal = Some(serde_json::json!({
            "template_id": "cowd/ephemeral-research-parent",
            "name": "临时研究父团队",
            "team_display_name": "研究",
            "roles": [{
                "role_id": "evidence_researcher",
                "display_name": "证据研究员",
                "responsibility": "收集并校验授权范围内的研究证据",
                "agent_definition_ref": "workspace/cowd/nonexistent@1",
                "grant_ceiling": ["read"],
                "fixed_count": 1,
                "acceptance": ["summary", "evidence"],
                "behavior": [{"kind": "reacquire_evidence", "required": true}]
            }],
            "result_fields": ["summary", "evidence"],
            "evidence_required": true,
            "instructions": "# 临时研究\n\n仅收集授权范围内的证据。"
        }));
        materialize_ephemeral_team_template(&mut seed_request, &services)
            .expect("custom parent Team snapshot materializes");
        seed_request.strategy_binding = Some(harness_contract::team::TeamStrategyBinding {
            decision_id: "patch-seed".to_string(),
            decision_revision: 1,
            decision_lease: "patch-seed-lease".to_string(),
            turn_ref: "turn-v621".to_string(),
        });
        team_authority::bind_semantic_resource_authority(
            &mut seed_request,
            None,
            services.workspace_root(),
        );
        ensure_test_team_resource(&mut seed_request);
        let seed_plan = planner::plan_runtime_orchestration(&seed_request);
        let mut seed_graph = services
            .compile_graph_agent_intents(
                compiler::compile_orchestration(
                    "patch-seed",
                    &seed_request,
                    &seed_plan,
                    None,
                    Some(services.team_runtime().as_ref()),
                )
                .expect("seed Team program compiles")
                .graph,
            )
            .expect("seed Agent intents compile");
        collaboration_coordinator::prepare_program_admission(
            &mut seed_graph,
            services.team_runtime().as_ref(),
        )
        .expect("seed Program admission compiles");
        let registered = services
            .commit_service()
            .register_graph(seed_graph)
            .expect("register active Program")
            .graph;
        let program = registered
            .orchestration
            .as_ref()
            .and_then(|metadata| metadata.collaboration_program.as_ref())
            .expect("registered Program");
        let escalation = harness_contract::execution_graph::CollaborationEscalationRequest {
            base_revision: program.revision,
            source_attempt: "team-agent:research:attempt:1".to_string(),
            request_kind: "add_team".to_string(),
            reason: "independent evidence review is required".to_string(),
            evidence_refs: Vec::new(),
            digest: "d".repeat(64),
            requested_add_team: Some(
                harness_contract::execution_graph::CollaborationEscalationAddTeam {
                    semantic_node_id: "independent-review".to_string(),
                    objective: "independently review the bounded research evidence".to_string(),
                    depends_on: vec!["research".to_string()],
                    resource_scopes: vec!["network:*".to_string()],
                    output_artifacts: vec!["independent-review".to_string()],
                    evidence_contract: vec!["summary".to_string(), "evidence".to_string()],
                    required: true,
                    parallelism_hint: 1,
                },
            ),
            template_proposal: Some(serde_json::json!({
                "template_id": "cowd/independent-review-snapshot",
                "name": "独立审查团队",
                "team_display_name": "独立审查",
                "roles": [{
                    "role_id": "evidence_reviewer",
                    "display_name": "独立审查员",
                    "responsibility": "独立复核授权证据并明确未解决风险",
                    "agent_definition_ref": "workspace/cowd/nonexistent@1",
                    "grant_ceiling": ["read"],
                    "fixed_count": 1,
                    "acceptance": ["summary", "evidence"],
                    "behavior": [{"kind": "verification", "mode": "independent"}]
                }],
                "result_fields": ["summary", "evidence"],
                "evidence_required": true,
                "instructions": "# 独立审查\n\n只依据授权证据复核结论，说明不确定性。"
            })),
        };
        escalation.validate().expect("typed escalation validates");
        let mut patch = escalation.as_add_team_patch(program.program_id.clone());
        attach_escalated_ephemeral_template(
            &registered,
            &mut patch,
            escalation
                .template_proposal
                .clone()
                .expect("custom template proposal"),
            &services,
        )
        .expect("Runtime binds the escalation custom template to its parent Program");
        let mut patch_request =
            collaboration_coordinator::compile_add_team_patch(&registered, &patch)
                .expect("fenced AddTeam patch compiles");
        team_authority::bind_semantic_resource_authority(
            &mut patch_request,
            None,
            services.workspace_root(),
        );
        let patch_plan = planner::plan_runtime_orchestration(&patch_request);
        let patch_proposal = patch_request.proposal.as_ref().expect("patch proposal");
        let existing_instances = program.semantic_node_instances.clone();
        let mut repairs = Vec::new();
        let mut mutation = compiler::compile_graph_mutation(
            "patch-revision",
            &patch_request,
            &patch_plan,
            patch_proposal,
            &registered.id,
            registered.parent_execution.as_ref(),
            services.team_runtime().as_ref(),
            &existing_instances,
            &mut repairs,
        )
        .expect("patch Team node compiles");
        assert!(repairs.is_empty());
        let compiled_team_request = serde_json::from_str::<
            harness_contract::team::TeamInstantiationRequest,
        >(&mutation.nodes[0].payload_ref)
        .expect("compiled custom Team request");
        assert!(matches!(
            compiled_team_request.template_selector,
            harness_contract::team::TeamTemplateSelector::Ephemeral { .. }
        ));
        services
            .compile_agent_task_nodes(&mut mutation.nodes)
            .expect("patch Agent intents compile");
        let mut delta = compiler::collaboration_program_from_proposal(
            patch_proposal,
            Some(&mutation.semantic_node_instances),
        )
        .expect("patch Program delta compiles")
        .expect("Team patch has a Program delta");
        collaboration_coordinator::prepare_program_revision_admission(
            &registered,
            &mut delta,
            mutation.nodes.clone(),
            services.team_runtime().as_ref(),
        )
        .expect("patch admission is fully prepared before commit");
        let completion = compiler::materialize_completion(
            &patch_proposal.completion,
            &mutation.semantic_node_instances,
            &patch_proposal.nodes,
        );
        let committed = services
            .commit_service()
            .replan_semantic(
                &registered,
                mutation.nodes.clone(),
                mutation.edges.clone(),
                patch_proposal.reason.clone(),
                patch_proposal.mutation_id.clone(),
                completion,
                Some(delta),
            )
            .expect("patch revision commits atomically")
            .graph;
        let revised = committed
            .orchestration
            .as_ref()
            .and_then(|metadata| metadata.collaboration_program.as_ref())
            .expect("revised Program");
        assert_eq!(revised.revision, program.revision + 1);
        assert_eq!(revised.team_instances.len(), 2);
        assert_eq!(revised.control.obligations.len(), 2);
        assert_eq!(
            revised.control.lifecycle,
            harness_contract::execution_graph::CollaborationProgramLifecycle::Admitting
        );
        assert!(revised.control.obligations.iter().all(|obligation| {
            obligation.revision == revised.revision
                && obligation.binding_ref.starts_with("team-binding:sha256:")
        }));
        assert!(revised
            .semantic_node_instances
            .contains_key("independent-review"));
        assert!(
            collaboration_coordinator::compile_add_team_patch(&committed, &patch)
                .is_err_and(|error| error == "patch_program_revision_conflict")
        );
    }

    #[tokio::test]
    async fn collaboration_coordinator_records_rejected_team_admission_as_typed_program_truth() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        ensure_test_mission(&services);
        let mut team = node("rejected-team", CapabilityRecipeId::Team, Vec::new());
        team.template = Some("cowd/parallel-research-synthesis".to_string());
        let mut request = proposal(vec![team]);
        request.strategy_binding = Some(harness_contract::team::TeamStrategyBinding {
            decision_id: "coordinator-rejection".to_string(),
            decision_revision: 1,
            decision_lease: "coordinator-rejection-lease".to_string(),
            turn_ref: "turn-v621".to_string(),
        });
        team_authority::bind_semantic_resource_authority(
            &mut request,
            None,
            services.workspace_root(),
        );
        ensure_test_team_resource(&mut request);
        let plan = planner::plan_runtime_orchestration(&request);
        let compiled = compiler::compile_orchestration(
            "coordinator-rejection",
            &request,
            &plan,
            None,
            Some(services.team_runtime().as_ref()),
        )
        .expect("Team root compiles");
        let mut graph = services
            .compile_graph_agent_intents(compiled.graph)
            .expect("agent intents compile");
        collaboration_coordinator::prepare_program_admission(
            &mut graph,
            services.team_runtime().as_ref(),
        )
        .expect("Program admission control compiles");
        let node_id = graph.nodes[0].id.clone();
        let registered = services
            .execution_supervisor()
            .register_graph(graph)
            .await
            .expect("register Program graph");
        collaboration_coordinator::mark_team_admission_rejected(
            &registered.id,
            &node_id,
            services.execution_supervisor().as_ref(),
            services.graph_state_store(),
        )
        .await
        .expect("typed rejection commits");
        let updated = services
            .graph_state_store()
            .load(&registered.id)
            .expect("load rejected Program");
        let control = &updated
            .orchestration
            .as_ref()
            .and_then(|metadata| metadata.collaboration_program.as_ref())
            .expect("Program")
            .control;
        assert_eq!(
            control.lifecycle,
            harness_contract::execution_graph::CollaborationProgramLifecycle::Blocked
        );
        assert_eq!(
            control.blocker_ref.as_deref(),
            Some(format!("execution-node:{node_id}").as_str())
        );
        assert_eq!(
            control.next_action.as_deref(),
            Some("inspect_team_admission_failure")
        );
        assert_eq!(
            control.obligations[0].state,
            harness_contract::execution_graph::TeamAdmissionState::BlockedPolicy
        );
        assert_eq!(
            control.obligations[0].reason_kind.as_deref(),
            Some("team_admission_rejected")
        );
    }
}
