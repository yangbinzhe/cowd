//! Model-visible semantic orchestration over Runtime-owned execution graphs.
//!
//! The model may inspect state and propose semantic topology. Runtime alone
//! resolves definitions, executors, leases, physical identities and commands.

pub mod compiler;
pub(crate) mod input_disposition;
pub mod planner;
pub mod request;
pub mod result;
pub(crate) mod team_authority;
pub mod validator;

use std::collections::BTreeSet;

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
use serde_json::{json, Value};

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
    team_authority::bind_semantic_resource_authority(
        &mut request,
        leased_decision,
        services.workspace_root(),
    );
    if request.constraints.risk.as_deref() == Some("critical")
        && request.constraints.approval_id.is_none()
    {
        if let Err(error) = submit_approval(&mut request, services) {
            return unavailable_result(&request, format!("approval_submission_failed:{error}"));
        }
    }
    let plan = planner::plan_runtime_orchestration_with_decision_and_resources(
        &request,
        leased_decision,
        crate::execution_core::StrategyResourceHealth {
            provider_available: true,
            tools_available: true,
            collaboration_available: true,
            mission_available: true,
            observed: true,
        },
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
    let graph = services
        .compile_graph_agent_intents(compiled.graph)
        .map_err(|error| format!("agent_binding_compilation_failed:{error}"))?;
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
    let run = services
        .execution_supervisor()
        .submit_and_wait(graph, compiled.command);
    let (_, report) = await_with_cancellation(run, cancellation, services, &graph_id).await?;
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
        .unwrap_or(8)
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
    let existing_ids = graph
        .nodes
        .iter()
        .map(|node| node.id.clone())
        .collect::<BTreeSet<_>>();
    let mut revision_repairs = Vec::new();
    let mut mutation = compiler::compile_graph_mutation(
        request_id,
        request,
        plan,
        proposal,
        graph_id,
        graph.parent_execution.as_ref(),
        services.team_runtime().as_ref(),
        &existing_ids,
        &mut revision_repairs,
    )
    .map_err(|error| format!("semantic_revision_compile_failed:{error}"))?;
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
    let expected_revision = proposal
        .expected_revision
        .ok_or_else(|| "revise_missing_expected_revision".to_string())?;
    let run = services.execution_supervisor().revise_semantic_graph(
        graph_id,
        expected_revision,
        mutation.nodes,
        mutation.edges,
        proposal.reason.clone(),
        proposal.mutation_id.clone(),
        completion,
    );
    let (_, report) = await_with_cancellation(run, cancellation, services, graph_id).await?;
    let projection = services
        .execution_supervisor()
        .projection(graph_id)
        .await
        .map_err(|error| format!("revision_projection_failed:{error}"))?;
    completed_projection(request.operation, projection, Some(report), false, services)
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
            .runtime_observed_resource_scopes
            .extend(node.usage.runtime_observed_resource_scopes.iter().cloned());
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
        let materialization = services
            .graph_state_store()
            .load(&child_graph_id)
            .map_err(|error| error.to_string())
            .and_then(|graph| working_state.verify_completed_graph(&graph));
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
    }
    assessment.team_ids.sort();
    assessment.team_ids.dedup();
    assessment.usage.runtime_write_attempt_paths.sort();
    assessment.usage.runtime_write_attempt_paths.dedup();
    assessment.usage.runtime_observed_resource_scopes.sort();
    assessment.usage.runtime_observed_resource_scopes.dedup();
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
            risk: TaskRisk::Critical,
            domain: harness_contract::policy::ApprovalDomain::Execution,
            blocks_execution: true,
            evidence_refs: request.evidence_refs.iter().take(64).cloned().collect(),
            timeout_policy: ApprovalTimeoutPolicy::Pending,
        },
    )?;
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
                reason: "parallel evidence lanes".to_string(),
            }),
            control: None,
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
}
