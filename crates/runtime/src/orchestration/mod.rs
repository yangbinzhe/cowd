//! Runtime orchestration is a pure intent-to-graph compiler.
//!
//! Stateful execution is owned exclusively by `ExecutionGraphRunner` through
//! `RuntimeServices`. This module must never dispatch tools, agents, teams,
//! sessions, missions, or approvals directly.

pub mod compiler;
pub mod planner;
pub mod request;
pub mod result;
pub mod validator;

use crate::execution_core::{graph::ExecutionRunReport, RuntimeExecutionDecision};
use crate::RuntimeServices;
use harness_contract::agent::{AgentTaskIntent, AgentTaskPacket};
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphProjection, ExecutionNodeKind, ExecutionNodeStatus,
};
use serde_json::{json, Value};

pub use compiler::CompiledOrchestration;
pub use planner::RuntimeOrchestrationPlan;
pub use request::{
    RuntimeOrchestrationAction, RuntimeOrchestrationConstraints, RuntimeOrchestrationRequest,
};
pub use result::{
    RuntimeOrchestrationApprovalRequirement, RuntimeOrchestrationDecision,
    RuntimeOrchestrationResult,
};

#[must_use]
pub fn handle_runtime_orchestration_request(
    request: RuntimeOrchestrationRequest,
) -> RuntimeOrchestrationResult {
    compile_runtime_orchestration_request(request, None, None, None)
}

#[must_use]
pub fn handle_runtime_orchestration_request_with_decision(
    request: RuntimeOrchestrationRequest,
    leased_decision: Option<&RuntimeExecutionDecision>,
) -> RuntimeOrchestrationResult {
    compile_runtime_orchestration_request(request, leased_decision, None, None)
}

/// Compile against workspace policy. Execution is owned by the active session
/// runtime, which binds provider, tool, agent, and session backends to its graph.
pub async fn submit_runtime_orchestration_request(
    request: RuntimeOrchestrationRequest,
    leased_decision: Option<&RuntimeExecutionDecision>,
    services: &RuntimeServices,
    parent_execution: Option<harness_contract::execution_graph::ExecutionParentBinding>,
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
    mut request: RuntimeOrchestrationRequest,
    leased_decision: Option<&RuntimeExecutionDecision>,
    services: &RuntimeServices,
    parent_execution: Option<harness_contract::execution_graph::ExecutionParentBinding>,
    cancellation: Option<crate::CancellationToken>,
) -> RuntimeOrchestrationResult {
    // Canonicalize the request before taking the reuse/locking snapshot.
    // Model-originated calls deliberately arrive without a self-reported
    // binding; the active Runtime decision supplies the only authoritative
    // collaboration lease.
    bind_team_request_to_strategy(&mut request, leased_decision, parent_execution.as_ref());
    let reuse_request = request.clone();
    let reuse_parent = parent_execution.clone();
    let (mut result, compiled) = match compile_runtime_orchestration(
        request,
        leased_decision,
        Some(services),
        parent_execution,
    ) {
        Ok(compiled) => compiled,
        Err(result) => return result,
    };
    let _collaboration_lease_guard =
        if reuse_request.action == RuntimeOrchestrationAction::RequestTeam {
            match (
                reuse_request.strategy_binding.as_ref(),
                reuse_parent.as_ref(),
            ) {
                (Some(binding), Some(parent)) => {
                    let scope = match crate::execution_core::graph::ScopedResource::resource(
                        "team-collaboration-lease",
                        format!(
                            "{}:{}:{}",
                            services.workspace_key(),
                            parent.execution_id,
                            binding.decision_lease
                        ),
                    ) {
                        Ok(scope) => scope,
                        Err(error) => {
                            result.status = "blocked".to_string();
                            result.decision.status = result.status.clone();
                            result
                                .decision
                                .validation_findings
                                .push(format!("collaboration_lease_scope_invalid:{error}"));
                            return result;
                        }
                    };
                    match services
                        .scope_locks()
                        .acquire(
                            [crate::execution_core::graph::ScopeLockRequest {
                                scope,
                                mode: crate::execution_core::graph::ScopeLockMode::Write,
                            }],
                            None,
                        )
                        .await
                    {
                        Ok(lease) => Some(lease),
                        Err(error) => {
                            result.status = "blocked".to_string();
                            result.decision.status = result.status.clone();
                            result
                                .decision
                                .validation_findings
                                .push(format!("collaboration_lease_claim_failed:{error}"));
                            return result;
                        }
                    }
                }
                _ => None,
            }
        } else {
            None
        };

    // A model can legitimately inspect the completed receipt and mention the
    // same team request again in a later model step. A second protocol graph
    // would repeat the work and spend another provider budget. Graph lineage
    // is the durable source of truth, so reuse is decided here rather than in
    // a Gateway adapter or an in-memory conversation cache.
    if let Some(reused) =
        find_reusable_team_execution(&reuse_request, services, reuse_parent.as_ref()).await
    {
        return apply_reused_team_execution(result, reused, services);
    }
    let Some(compiled) = compiled else {
        return result;
    };
    if !should_execute(&result, &compiled) {
        return result;
    }

    let team_request = compiled.team_request.clone();
    let graph = if team_request.is_some() {
        // TeamInstantiationService already compiled exact Agent Bindings while
        // producing the dry-run graph. The actual start below deliberately
        // re-enters TeamRuntime::instantiate with the same immutable request.
        compiled.graph
    } else {
        match services.compile_graph_agent_intents(compiled.graph) {
            Ok(graph) => graph,
            Err(error) => {
                result.status = "blocked".to_string();
                result.decision.status = result.status.clone();
                result
                    .decision
                    .validation_findings
                    .push(format!("agent_binding_compilation_failed:{error}"));
                return result;
            }
        }
    };
    let graph_id = graph.id.clone();
    let compiled_team = declares_team(&graph);
    let compiled_team_id = team_id(&graph);
    let automatic_team =
        reuse_request.selection_mode == Some(harness_contract::team::TeamSelectionMode::Automatic);
    if automatic_team
        && graph
            .nodes
            .iter()
            .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
            .count()
            < 2
    {
        result.status = "blocked".to_string();
        result.decision.status = result.status.clone();
        result
            .decision
            .validation_findings
            .push("automatic_team_requires_at_least_two_bounded_agent_slots".to_string());
        return result;
    }
    let run_future = async {
        if let Some(team_request) = team_request {
            services
                .team_runtime()
                .instantiate(team_request)
                .await
                .map(|_| ())
        } else {
            services
                .graph_runner()
                .start(graph)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
    };
    tokio::pin!(run_future);
    let run = if let Some(cancellation) = cancellation {
        tokio::select! {
            run = &mut run_future => run,
            () = cancellation.cancelled() => {
                match cancel_orchestration_execution(services, &graph_id).await {
                    Ok(()) => Err("parent conversation cancelled automatic Team execution; child graph and Agents reached terminal cancellation".to_string()),
                    Err(error) => Err(format!(
                        "parent conversation cancellation propagation failed: {error}"
                    )),
                }
            }
        }
    } else {
        run_future.await
    };
    match run {
        Ok(()) => match services.graph_runner().projection(&graph_id).await {
            Ok(projection) => {
                let report = report_from_projection(&projection);
                let terminal = projection.terminal_result_ref.clone();
                result.status = if report.failed > 0 || report.blocked > 0 {
                    "blocked".to_string()
                } else if report.waiting > 0 {
                    "waiting".to_string()
                } else if terminal.is_none() {
                    result
                        .decision
                        .validation_findings
                        .push("execution_graph_quiesced_without_terminal_result".to_string());
                    "blocked".to_string()
                } else {
                    "completed".to_string()
                };
                result.decision.status = result.status.clone();
                result.execution = json!({
                    "type": "execution_graph_run",
                    "status": result.status,
                    "report": report,
                    "projection": projection,
                    "terminal_result_ref": terminal,
                });
                result.evidence["accepted"] = Value::Bool(result.status == "completed");
                result.evidence["executed"] = Value::Bool(true);
                result.evidence["graph_id"] = Value::String(graph_id.clone());
                if compiled_team && result.status == "completed" {
                    match compiled_team_id.as_deref().map(|team_id| {
                        services
                            .team_runtime()
                            .working_state(team_id)
                            .map(|state| (team_id.to_string(), state))
                    }) {
                        Some(Ok((team_id, working_state))) if !working_state.entries.is_empty() => {
                            let overlap = working_state.focus_overlap_assessment();
                            result.execution["team_working_state"] =
                                serde_json::to_value(&working_state).unwrap_or(Value::Null);
                            result.execution["focus_overlap_assessment"] =
                                serde_json::to_value(&overlap).unwrap_or(Value::Null);
                            result.evidence["team_id"] = Value::String(team_id);
                            let materialization = services
                                .graph_state_store()
                                .load(&graph_id)
                                .map_err(|error| error.to_string())
                                .and_then(|graph| working_state.verify_completed_graph(&graph));
                            result.evidence["working_state_verified"] =
                                Value::Bool(materialization.is_ok());
                            result.evidence["focus_overlap_verified"] =
                                Value::Bool(overlap.observed);
                            result.evidence["focus_overlap_exceeded"] =
                                Value::Bool(overlap.exceeded);
                            if let Err(error) = materialization {
                                result.status = "blocked".to_string();
                                result.decision.status = result.status.clone();
                                result
                                    .decision
                                    .validation_findings
                                    .push(format!("team_working_state_not_materialized:{error}"));
                                result.evidence["accepted"] = Value::Bool(false);
                            } else if overlap.exceeded {
                                result.status = "blocked".to_string();
                                result.decision.status = result.status.clone();
                                result.decision.validation_findings.push(format!(
                                    "team_focus_overlap_budget_exceeded:{}bp>{}bp",
                                    overlap.maximum_overlap_bp, overlap.allowed_overlap_bp
                                ));
                                result.evidence["accepted"] = Value::Bool(false);
                            }
                        }
                        Some(Ok(_)) | None => {
                            result.status = "blocked".to_string();
                            result.decision.status = result.status.clone();
                            result
                                .decision
                                .validation_findings
                                .push("team_terminal_missing_committed_working_state".to_string());
                            result.evidence["accepted"] = Value::Bool(false);
                        }
                        Some(Err(error)) => {
                            result.status = "blocked".to_string();
                            result.decision.status = result.status.clone();
                            result
                                .decision
                                .validation_findings
                                .push(format!("team_working_state_unavailable:{error}"));
                            result.evidence["accepted"] = Value::Bool(false);
                        }
                    }
                    result.next_model_guidance = if result.status == "completed" {
                        "The requested team completed in this turn. Treat the terminal summary and durable graph evidence as the canonical result; do not create another overlapping team or deliberation graph unless the user introduces a genuinely independent objective."
                            .to_string()
                    } else {
                        "The Team executed but failed a Runtime collaboration contract. Preserve its durable evidence, surface the validation finding, and use the governed fallback without creating a duplicate Team."
                            .to_string()
                    };
                }
            }
            Err(error) => {
                result.status = "blocked".to_string();
                result.decision.status = result.status.clone();
                result
                    .decision
                    .validation_findings
                    .push(format!("execution_projection_unavailable:{error}"));
            }
        },
        Err(error) => {
            result.status = "blocked".to_string();
            result.decision.status = result.status.clone();
            result
                .decision
                .validation_findings
                .push(format!("execution_graph_run_failed:{error}"));
            if error.starts_with("parent conversation cancelled automatic Team execution") {
                result.evidence["executed"] = Value::Bool(true);
                result.evidence["accepted"] = Value::Bool(false);
                result.evidence["graph_id"] = Value::String(graph_id);
            }
        }
    }
    result
}

async fn cancel_orchestration_execution(
    services: &RuntimeServices,
    graph_id: &str,
) -> Result<(), String> {
    // Registration and cancellation can race by one scheduler yield. Retry
    // only the durable lookup; once the graph exists, Runner cancellation
    // invokes active node executor cancellation before atomically marking all
    // remaining nodes terminal.
    let mut last_error = None;
    for _ in 0..100 {
        match services.graph_state_store().load_async(graph_id).await {
            Ok(graph) => {
                if graph
                    .node_statuses
                    .values()
                    .all(|status| status.is_terminal())
                {
                    return Ok(());
                }
                match services
                    .graph_runner()
                    .command(
                        graph_id,
                        harness_contract::execution_graph::ExecutionGraphCommand::Cancel {
                            expected_revision: graph.revision,
                            reason:
                                "parent conversation cancellation propagated to child execution"
                                    .to_string(),
                        },
                    )
                    .await
                {
                    Ok(cancelled)
                        if cancelled
                            .node_statuses
                            .values()
                            .all(|status| status.is_terminal()) =>
                    {
                        return Ok(());
                    }
                    Ok(_) => {
                        last_error =
                            Some("Runner cancel returned a non-terminal child graph".to_string());
                    }
                    Err(error) => last_error = Some(error.to_string()),
                }
            }
            Err(error) => last_error = Some(error.to_string()),
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    Err(format!(
        "child graph `{graph_id}` cancellation did not commit terminal lineage: {}",
        last_error.unwrap_or_else(|| "graph registration unavailable".to_string())
    ))
}

#[derive(Debug)]
struct ReusedTeamExecution {
    request_id: String,
    graph_id: String,
    projection: ExecutionGraphProjection,
    team_id: Option<String>,
}

async fn find_reusable_team_execution(
    request: &RuntimeOrchestrationRequest,
    services: &RuntimeServices,
    parent: Option<&harness_contract::execution_graph::ExecutionParentBinding>,
) -> Option<ReusedTeamExecution> {
    if request.action != RuntimeOrchestrationAction::RequestTeam {
        return None;
    }
    let parent = parent?;
    let links = services
        .graph_state_store()
        .child_links_async(parent.execution_id.clone())
        .await
        .ok()?;
    let requested_lease = request
        .strategy_binding
        .as_ref()
        .map(|binding| binding.decision_lease.as_str());
    for link in links {
        if requested_lease.is_none() && !same_team_objective(&link.child_objective, &request.intent)
        {
            continue;
        }
        let graph = services
            .graph_state_store()
            .load_async(link.child_execution_id.clone())
            .await
            .ok()?;
        if !declares_team(&graph)
            || requested_lease
                .is_some_and(|lease| team_collaboration_lease(&graph).as_deref() != Some(lease))
            || (requested_lease.is_none()
                && !same_team_objective(&graph.objective, &request.intent))
        {
            continue;
        }
        let projection = services
            .graph_state_store()
            .projection_async(link.child_execution_id.clone())
            .await
            .ok()?;
        let team_id = team_id(&graph);
        let request_id = team_id
            .as_deref()
            .and_then(|team_id| team_id.strip_prefix("runtime-team:"))
            .filter(|request_id| !request_id.trim().is_empty())?
            .to_string();
        return Some(ReusedTeamExecution {
            request_id,
            graph_id: link.child_execution_id,
            projection,
            team_id,
        });
    }
    None
}

fn team_collaboration_lease(graph: &ExecutionGraph) -> Option<String> {
    graph
        .nodes
        .iter()
        .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
        .filter_map(|node| task_packet_or_intent(&node.payload_ref))
        .flat_map(|(constraints, _)| constraints)
        .find_map(|constraint| {
            constraint
                .strip_prefix("collaboration_lease:")
                .map(str::to_string)
        })
}

fn apply_reused_team_execution(
    mut result: RuntimeOrchestrationResult,
    reused: ReusedTeamExecution,
    services: &RuntimeServices,
) -> RuntimeOrchestrationResult {
    // A replay is the original collaboration receipt, not a new request that
    // happens to point at the old graph. Preserve its canonical request
    // identity so all surfaces converge on the same idempotent receipt.
    result.request_id = reused.request_id.clone();
    result.evidence["request_id"] = Value::String(reused.request_id.clone());
    let report = report_from_projection(&reused.projection);
    let terminal = reused.projection.terminal_result_ref.clone();
    result.status = if report.failed > 0 || report.blocked > 0 {
        "blocked".to_string()
    } else if report.waiting > 0 || terminal.is_none() {
        "waiting".to_string()
    } else {
        "completed".to_string()
    };
    result.decision.status = result.status.clone();
    result.execution = json!({
        "type": "execution_graph_reused",
        "status": result.status,
        "report": report,
        "projection": reused.projection,
        "terminal_result_ref": terminal,
        "reused_from_graph_id": reused.graph_id.clone(),
        "original_request_id": reused.request_id.clone(),
    });
    result.evidence["accepted"] = Value::Bool(result.status == "completed");
    result.evidence["executed"] = Value::Bool(true);
    result.evidence["reused"] = Value::Bool(true);
    result.evidence["graph_id"] = Value::String(reused.graph_id.clone());
    if result.status == "completed" {
        match reused.team_id.as_deref().map(|team_id| {
            services
                .team_runtime()
                .working_state(team_id)
                .map(|state| (team_id.to_string(), state))
        }) {
            Some(Ok((team_id, working_state))) if !working_state.entries.is_empty() => {
                let overlap = working_state.focus_overlap_assessment();
                result.execution["team_working_state"] =
                    serde_json::to_value(&working_state).unwrap_or(Value::Null);
                result.execution["focus_overlap_assessment"] =
                    serde_json::to_value(&overlap).unwrap_or(Value::Null);
                result.evidence["team_id"] = Value::String(team_id);
                let materialization = services
                    .graph_state_store()
                    .load(&reused.graph_id)
                    .map_err(|error| error.to_string())
                    .and_then(|graph| working_state.verify_completed_graph(&graph));
                result.evidence["working_state_verified"] = Value::Bool(materialization.is_ok());
                result.evidence["focus_overlap_verified"] = Value::Bool(overlap.observed);
                result.evidence["focus_overlap_exceeded"] = Value::Bool(overlap.exceeded);
                if let Err(error) = materialization {
                    result.status = "blocked".to_string();
                    result.decision.status = result.status.clone();
                    result.decision.validation_findings.push(format!(
                        "replayed_team_working_state_not_materialized:{error}"
                    ));
                    result.evidence["accepted"] = Value::Bool(false);
                } else if overlap.exceeded {
                    result.status = "blocked".to_string();
                    result.decision.status = result.status.clone();
                    result.decision.validation_findings.push(format!(
                        "replayed_team_focus_overlap_budget_exceeded:{}bp>{}bp",
                        overlap.maximum_overlap_bp, overlap.allowed_overlap_bp
                    ));
                    result.evidence["accepted"] = Value::Bool(false);
                }
            }
            Some(Ok(_)) | None => {
                result.status = "blocked".to_string();
                result.decision.status = result.status.clone();
                result
                    .decision
                    .validation_findings
                    .push("replayed_team_missing_committed_working_state".to_string());
                result.evidence["accepted"] = Value::Bool(false);
            }
            Some(Err(error)) => {
                result.status = "blocked".to_string();
                result.decision.status = result.status.clone();
                result
                    .decision
                    .validation_findings
                    .push(format!("replayed_team_working_state_unavailable:{error}"));
                result.evidence["accepted"] = Value::Bool(false);
            }
        }
    }
    result.next_model_guidance = if result.status == "completed" {
        "The requested team already completed in this turn. Use its terminal receipt as evidence; do not request the same team again unless the user changes the objective."
            .to_string()
    } else {
        "The requested team is already active in this turn. Inspect its durable execution projection and wait for or advance that work; do not create a duplicate team."
            .to_string()
    };
    result
}

fn declares_team(graph: &ExecutionGraph) -> bool {
    graph.nodes.iter().any(|node| {
        node.kind == ExecutionNodeKind::AgentTask
            && task_packet_or_intent(&node.payload_ref)
                .and_then(|(_, team_id)| team_id)
                .is_some_and(|team_id| !team_id.trim().is_empty())
    })
}

fn team_id(graph: &ExecutionGraph) -> Option<String> {
    graph
        .nodes
        .iter()
        .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
        .filter_map(|node| task_packet_or_intent(&node.payload_ref))
        .find_map(|(_, team_id)| team_id)
        .filter(|team_id| !team_id.trim().is_empty())
}

fn task_packet_or_intent(payload: &str) -> Option<(Vec<String>, Option<String>)> {
    if let Ok(packet) = serde_json::from_str::<AgentTaskPacket>(payload) {
        return Some((packet.constraints, packet.team_id));
    }
    serde_json::from_str::<AgentTaskIntent>(payload)
        .ok()
        .map(|intent| (intent.constraints, intent.team_id))
}

fn same_objective(left: &str, right: &str) -> bool {
    normalize_objective(left) == normalize_objective(right)
}

/// Team graphs are scoped to one parent execution. A model often rephrases
/// the same cross-domain objective after receiving a team receipt, sometimes
/// even switching `request_team` to `request_deliberation`. The durable
/// parent graph is still the same user turn, so reuse a materially
/// overlapping team rather than spending another fan-out. New, independent
/// work remains possible because it will not share enough objective terms.
fn same_team_objective(left: &str, right: &str) -> bool {
    if same_objective(left, right) {
        return true;
    }
    let left_terms = objective_terms(left);
    let right_terms = objective_terms(right);
    if left_terms.is_empty() || right_terms.is_empty() {
        return false;
    }
    let overlap = left_terms.intersection(&right_terms).count();
    let smallest = left_terms.len().min(right_terms.len());
    overlap >= 3 && overlap.saturating_mul(100) >= smallest.saturating_mul(40)
}

fn objective_terms(value: &str) -> std::collections::BTreeSet<String> {
    let normalized = value.to_lowercase();
    let mut terms = std::collections::BTreeSet::new();
    let mut ascii = String::new();
    let mut cjk = Vec::new();
    let flush_ascii = |ascii: &mut String, terms: &mut std::collections::BTreeSet<String>| {
        if ascii.len() >= 2 {
            terms.insert(format!("word:{ascii}"));
        }
        ascii.clear();
    };
    for character in normalized.chars() {
        if character.is_ascii_alphanumeric() {
            ascii.push(character);
            continue;
        }
        flush_ascii(&mut ascii, &mut terms);
        if is_cjk(character) {
            cjk.push(character);
        } else {
            cjk.push(' ');
        }
    }
    flush_ascii(&mut ascii, &mut terms);
    for pair in cjk.windows(2) {
        if pair.iter().all(|character| is_cjk(*character)) {
            terms.insert(format!("cjk:{}{}", pair[0], pair[1]));
        }
    }
    terms
}

const fn is_cjk(character: char) -> bool {
    matches!(character as u32, 0x3400..=0x4dbf | 0x4e00..=0x9fff | 0xf900..=0xfaff)
}

fn normalize_objective(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn report_from_projection(projection: &ExecutionGraphProjection) -> ExecutionRunReport {
    let mut report = ExecutionRunReport {
        graph_id: projection.graph_id.clone(),
        revision: projection.revision,
        completed: 0,
        failed: 0,
        blocked: 0,
        cancelled: 0,
        waiting: 0,
    };
    for node in &projection.nodes {
        match node.status {
            ExecutionNodeStatus::Completed => report.completed += 1,
            ExecutionNodeStatus::Failed => report.failed += 1,
            ExecutionNodeStatus::Blocked => report.blocked += 1,
            ExecutionNodeStatus::Cancelled => report.cancelled += 1,
            ExecutionNodeStatus::Planned
            | ExecutionNodeStatus::Ready
            | ExecutionNodeStatus::Running
            | ExecutionNodeStatus::WaitingInput
            | ExecutionNodeStatus::WaitingApproval
            | ExecutionNodeStatus::WaitingExternal
            | ExecutionNodeStatus::Paused => report.waiting += 1,
        }
    }
    report
}

fn should_execute(result: &RuntimeOrchestrationResult, compiled: &CompiledOrchestration) -> bool {
    result.status == "compiled"
        && compiled.execute_without_protocol
        && result.decision.status == "compiled"
}

fn compile_runtime_orchestration_request(
    request: RuntimeOrchestrationRequest,
    leased_decision: Option<&RuntimeExecutionDecision>,
    services: Option<&RuntimeServices>,
    parent_execution: Option<harness_contract::execution_graph::ExecutionParentBinding>,
) -> RuntimeOrchestrationResult {
    compile_runtime_orchestration(request, leased_decision, services, parent_execution)
        .map(|(result, _)| result)
        .unwrap_or_else(|result| result)
}

fn compile_runtime_orchestration(
    mut request: RuntimeOrchestrationRequest,
    leased_decision: Option<&RuntimeExecutionDecision>,
    services: Option<&RuntimeServices>,
    parent_execution: Option<harness_contract::execution_graph::ExecutionParentBinding>,
) -> Result<(RuntimeOrchestrationResult, Option<CompiledOrchestration>), RuntimeOrchestrationResult>
{
    bind_team_request_to_strategy(&mut request, leased_decision, parent_execution.as_ref());
    let resource_health = crate::execution_core::StrategyResourceHealth {
        provider_available: true,
        // Compilation describes required capabilities. Executor availability is
        // validated by the Runner registry before any state transition.
        tools_available: true,
        collaboration_available: true,
        mission_available: true,
        observed: true,
    };
    let plan = planner::plan_runtime_orchestration_with_decision_and_resources(
        &request,
        leased_decision,
        resource_health,
    );
    let mut decision = validator::validate_request(
        &request,
        &plan.execution_decision,
        plan.model_proposal.as_ref(),
        services.map(|services| services.approval_queue().as_ref()),
    );
    let request_id = format!("runtime-orch-{}", uuid::Uuid::new_v4());
    let compiled = if decision.status == "rejected" || decision.status == "needs_approval" {
        None
    } else {
        match compiler::compile_orchestration(
            &request_id,
            &request,
            &plan,
            parent_execution,
            services.map(|services| services.team_runtime().as_ref()),
        ) {
            Ok(compiled) => Some(compiled),
            Err(error) => {
                decision.status = "unavailable".to_string();
                decision
                    .validation_findings
                    .push(format!("execution_capability_unavailable:{error}"));
                None
            }
        }
    };
    let compiled_ok = compiled.is_some();
    let status = if decision.status == "rejected" || decision.status == "needs_approval" {
        decision.status.clone()
    } else if compiled_ok {
        "compiled".to_string()
    } else {
        "unavailable".to_string()
    };
    decision.status = status.clone();
    let execution = compiled.as_ref().map_or_else(
        || {
            json!({
                "type": "orchestration_not_submitted",
                "status": status,
                "findings": decision.validation_findings,
            })
        },
        |compiled| {
            json!({
                "type": "execution_graph_compilation",
                "status": "compiled",
                "graph": compiled.graph,
                "command": compiled.command,
            })
        },
    );
    let evidence = orchestration_evidence(&request_id, &request, &plan, &decision, compiled_ok);
    Ok((
        RuntimeOrchestrationResult {
            request_id,
            status,
            decision,
            execution,
            evidence,
            next_model_guidance: compiler::guidance_for_compile_result(compiled_ok),
        },
        compiled,
    ))
}

fn bind_team_request_to_strategy(
    request: &mut RuntimeOrchestrationRequest,
    leased_decision: Option<&RuntimeExecutionDecision>,
    parent_execution: Option<&harness_contract::execution_graph::ExecutionParentBinding>,
) {
    if request.action != RuntimeOrchestrationAction::RequestTeam {
        return;
    }
    let Some(decision) = leased_decision else {
        return;
    };
    if request.selection_mode.is_none() {
        request.selection_mode = Some(if decision.strategy.understanding.requests_multi_agent {
            harness_contract::team::TeamSelectionMode::Explicit
        } else {
            harness_contract::team::TeamSelectionMode::ModelAssisted
        });
    }
    if request.strategy_binding.is_none() {
        request.strategy_binding = Some(harness_contract::team::TeamStrategyBinding {
            decision_id: decision.decision_id.clone(),
            decision_revision: decision.decision_revision,
            decision_lease: decision.lease.lease_id.clone(),
            turn_ref: decision
                .turn_ref
                .clone()
                .or_else(|| parent_execution.map(|parent| parent.execution_id.clone()))
                .unwrap_or_else(|| "detached-orchestration".to_string()),
        });
    }
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
    match serde_json::from_value::<RuntimeOrchestrationRequest>(value) {
        Ok(request) => serde_json::to_value(handle_runtime_orchestration_request_with_decision(
            request,
            leased_decision,
        ))
        .unwrap_or_else(|error| json!({"status":"rejected","error":error.to_string()})),
        Err(error) => {
            json!({"status":"rejected","error":format!("invalid runtime_orchestrate input: {error}")})
        }
    }
}

fn orchestration_evidence(
    request_id: &str,
    request: &RuntimeOrchestrationRequest,
    plan: &RuntimeOrchestrationPlan,
    decision: &RuntimeOrchestrationDecision,
    compiled: bool,
) -> Value {
    json!({
        "type": "runtime_orchestration_evidence",
        "request_id": request_id,
        "action": request.action,
        "status": decision.status,
        "model_intent": request.intent,
        "selected_pattern": decision.selected_pattern.as_str(),
        "compile_target": plan.execution_decision.compile_target,
        "strategy_lease": plan.execution_decision.lease,
        "policy_gates": decision.policy_gates,
        "validation_findings": decision.validation_findings,
        "accepted": false,
        "compiled": compiled,
        "runtime_owner": "runtime.execution_graph_compiler",
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use harness_contract::agent::{AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus};
    use memory::SessionRecord;

    use super::*;

    struct CompletedProtocolBackend {
        objectives: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl crate::AgentRuntimeBackend for CompletedProtocolBackend {
        fn kind(&self) -> crate::AgentBackendKind {
            crate::AgentBackendKind::InProcess
        }

        fn capabilities(&self) -> crate::AgentBackendCapabilities {
            crate::AgentBackendCapabilities::in_process()
        }

        async fn execute(
            &self,
            packet: AgentTaskPacket,
            selection: crate::AgentModelSelection,
        ) -> Result<AgentReturnPacket, String> {
            self.objectives
                .lock()
                .expect("objectives")
                .push(packet.objective.clone());
            let evidence_id = format!("materialized:{}", packet.node_id);
            let evidence = harness_contract::context::EvidenceAccessRef::durable(
                harness_contract::context::EvidenceRef::new("tool", evidence_id),
                "a".repeat(64),
                1,
                "application/json",
                "artifact://art_orchestration_packet",
                format!("session:{}", packet.session_id),
            );
            let mut evidence_refs = packet.evidence_refs.clone();
            evidence_refs.push(evidence);
            let runtime_change_receipts = packet
                .acceptance
                .iter()
                .any(|criterion| matches!(criterion.as_str(), "implementation" | "mitigation"))
                .then(|| {
                    vec![harness_contract::agent::AgentChangeReceipt {
                        path: packet
                            .resource_scopes
                            .first()
                            .cloned()
                            .unwrap_or_else(|| "fixture.txt".to_string()),
                        before_sha256: Some("b".repeat(64)),
                        after_sha256: "c".repeat(64),
                        write_sequence: 1,
                    }]
                })
                .unwrap_or_default();
            let changes = runtime_change_receipts
                .iter()
                .map(|receipt| receipt.path.clone())
                .collect();
            Ok(AgentReturnPacket {
                run_id: packet.run_id,
                agent_id: packet.agent_id,
                task_id: packet.task_id,
                session_id: packet.session_id,
                mission_id: packet.mission_id,
                team_id: packet.team_id,
                graph_id: packet.graph_id,
                node_id: packet.node_id,
                attempt: packet.attempt,
                expected_graph_revision: packet.expected_graph_revision,
                status: AgentTerminalStatus::Completed,
                outcome: serde_json::json!({
                    "summary": "protocol agent completed",
                    "findings": ["fixture finding"],
                    "plan": "fixture plan",
                    "implementation": "fixture change",
                    "source_verification": "fixture source inspection",
                    "review": "fixture upstream review",
                    "risks": ["fixture risk"],
                    "unresolved": ["fixture gap"],
                    "proposal": "fixture proposal",
                    "critique": "fixture critique",
                    "mitigation": "fixture mitigation",
                    "checkpoint": "fixture checkpoint"
                })
                .to_string(),
                acceptance: packet.acceptance,
                evidence_refs,
                changes,
                runtime_change_receipts,
                conflicts: Vec::new(),
                unresolved: Vec::new(),
                input_tokens: 11,
                output_tokens: 7,
                cached_tokens: 0,
                model: selection.model,
                provider: selection.provider,
                tool_calls: 1,
                duplicate_tool_calls: 0,
                runtime_write_attempt_paths: Vec::new(),
                runtime_observed_resource_scopes: Vec::new(),
                failure: None,
            })
        }
    }

    fn request(action: RuntimeOrchestrationAction) -> RuntimeOrchestrationRequest {
        RuntimeOrchestrationRequest {
            intent: "inspect the workspace".to_string(),
            model_lease: None,
            session_id: Some("session-1".to_string()),
            target_session_id: None,
            action,
            selection_mode: None,
            strategy_binding: None,
            reason: None,
            template_hint: None,
            focus_partition_plans: Vec::new(),
            capabilities: vec!["resource:read:crates/runtime".to_string()],
            evidence_refs: Vec::new(),
            constraints: Default::default(),
            surface: None,
        }
    }

    #[test]
    fn sync_orchestration_compiles_without_side_effects() {
        let result =
            handle_runtime_orchestration_request(request(RuntimeOrchestrationAction::PlanOnly));
        assert_eq!(
            result.status, "compiled",
            "findings={:?}",
            result.decision.validation_findings
        );
        assert_eq!(result.execution["type"], "execution_graph_compilation");
        assert_eq!(result.evidence["accepted"], false);
    }

    #[test]
    fn rephrased_cross_domain_team_objectives_share_one_parent_execution() {
        assert!(same_team_objective(
            "复杂架构审查：启动团队分析 runtime、memory、gateway 的职责边界、事件真相和源码路径证据",
            "继续架构审查：基于已收集证据分析 gateway、runtime、memory 的 canonical state、风险和实际源码路径",
        ));
        assert!(!same_team_objective(
            "审查 runtime、memory、gateway 的架构边界",
            "为市场部门起草一份下季度招聘计划",
        ));
    }

    #[test]
    fn future_capability_is_typed_unavailable_not_fake_running() {
        let result =
            handle_runtime_orchestration_request(request(RuntimeOrchestrationAction::RequestTeam));
        assert_ne!(result.status, "running");
        assert_eq!(result.evidence["accepted"], false);
    }

    #[tokio::test]
    async fn async_orchestration_does_not_install_placeholder_executors() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let result = submit_runtime_orchestration_request(
            request(RuntimeOrchestrationAction::PlanOnly),
            None,
            services.as_ref(),
            None,
        )
        .await;

        assert_eq!(result.status, "compiled", "{result:?}");
        assert_eq!(result.evidence["accepted"], false);
        assert!(!services
            .event_store()
            .all_events(100)
            .expect("runtime events")
            .iter()
            .any(|event| event.kind == "execution_graph.planned"));
    }

    #[tokio::test]
    async fn dispatch_session_compiles_and_starts_the_canonical_handoff_graph() {
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let timestamp = chrono::Utc::now().to_rfc3339();
        for session_id in ["session-1", "session-2"] {
            store
                .create_session(&SessionRecord {
                    session_id: session_id.to_string(),
                    platform: "test".to_string(),
                    chat_id: format!("chat-{session_id}"),
                    user_id: None,
                    model: None,
                    created_at: timestamp.clone(),
                    last_activity: timestamp.clone(),
                    message_count: 0,
                    reset_policy: "manual".to_string(),
                    metadata_json: None,
                    input_tokens: 0,
                    output_tokens: 0,
                    estimated_cost_usd: 0.0,
                    status: "active".to_string(),
                })
                .await
                .unwrap();
        }
        let services = RuntimeServices::in_memory().expect("runtime services");
        services.install_session_store(Arc::clone(&store)).unwrap();
        let mut request = request(RuntimeOrchestrationAction::DispatchSession);
        request.target_session_id = Some("session-2".to_string());
        request.evidence_refs = vec!["evidence:source".to_string()];

        let result =
            submit_runtime_orchestration_request(request, None, services.as_ref(), None).await;

        assert_eq!(result.status, "waiting", "{result:?}");
        assert_eq!(result.execution["type"], "execution_graph_run");
        let target = store
            .claim_session_runtime_outbox(
                "orchestration-test",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
                    + 1_000,
                1_000,
                8,
            )
            .await
            .unwrap();
        assert_eq!(target.len(), 1);
        assert_eq!(target[0].session_id, "session-2");
    }

    #[tokio::test]
    async fn deliberation_instantiates_the_template_graph_and_returns_one_terminal_result() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workspace = temp.path().join("workspace");
        for relative in ["crates/runtime", "crates/gateway", "surfaces/webui"] {
            std::fs::create_dir_all(workspace.join(relative)).expect("bounded workspace scope");
        }
        let providers = crate::config::ProvidersConfig {
            providers: HashMap::from([(
                "test".to_string(),
                crate::config::ProviderConfig {
                    name: "test".to_string(),
                    base_url: "https://example.test/v1".to_string(),
                    api_key: "test".to_string(),
                    models: vec!["fast".to_string()],
                    protocol: Some("responses".to_string()),
                },
            )]),
        };
        let services = RuntimeServices::builder(temp.path(), &workspace)
            .provider_registry(Arc::new(
                crate::ProviderRegistry::new(providers).expect("provider registry"),
            ))
            .build()
            .expect("runtime services");
        let objectives = Arc::new(Mutex::new(Vec::new()));
        services
            .agent_runtime()
            .register_backend(Arc::new(CompletedProtocolBackend {
                objectives: Arc::clone(&objectives),
            }));

        let result = submit_runtime_orchestration_request(
            request(RuntimeOrchestrationAction::RequestDeliberation),
            None,
            services.as_ref(),
            None,
        )
        .await;

        assert_eq!(result.status, "completed", "{result:?}");
        assert!(result.execution.get("protocol").is_none());
        assert_eq!(result.execution["type"], "execution_graph_run");
        assert!(result.execution["terminal_result_ref"]
            .as_str()
            .is_some_and(|value| value.starts_with("assistant_json:")));
        assert_eq!(result.evidence["accepted"], true);
        assert!(services
            .event_store()
            .all_events(200)
            .expect("runtime events")
            .iter()
            .any(|event| event.kind == "agent.terminal"));
        assert!(objectives
            .lock()
            .expect("objectives")
            .iter()
            .any(|objective| {
                objective.contains("## Canonical upstream results")
                    && objective.contains("### Upstream proposer")
            }));
    }

    #[tokio::test]
    async fn same_parent_team_request_reuses_the_durable_protocol_graph() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let providers = crate::config::ProvidersConfig {
            providers: HashMap::from([(
                "test".to_string(),
                crate::config::ProviderConfig {
                    name: "test".to_string(),
                    base_url: "https://example.test/v1".to_string(),
                    api_key: "test".to_string(),
                    models: vec!["fast".to_string()],
                    protocol: Some("responses".to_string()),
                },
            )]),
        };
        let services = RuntimeServices::builder(temp.path(), &workspace)
            .provider_registry(Arc::new(
                crate::ProviderRegistry::new(providers).expect("provider registry"),
            ))
            .build()
            .expect("runtime services");
        let objectives = Arc::new(Mutex::new(Vec::new()));
        services
            .agent_runtime()
            .register_backend(Arc::new(CompletedProtocolBackend {
                objectives: Arc::clone(&objectives),
            }));
        let parent = harness_contract::execution_graph::ExecutionParentBinding {
            execution_id: "turn-graph-1".to_string(),
            node_id: "model-step-1".to_string(),
        };

        let first = submit_runtime_orchestration_request(
            request(RuntimeOrchestrationAction::RequestTeam),
            None,
            services.as_ref(),
            Some(parent.clone()),
        )
        .await;
        let executed_objectives = objectives.lock().expect("objectives").len();
        let second = submit_runtime_orchestration_request(
            request(RuntimeOrchestrationAction::RequestTeam),
            None,
            services.as_ref(),
            Some(parent),
        )
        .await;

        assert_eq!(first.status, "completed", "{first:?}");
        assert_eq!(second.status, "completed", "{second:?}");
        assert_eq!(second.execution["type"], "execution_graph_reused");
        assert_eq!(second.evidence["reused"], true);
        assert_eq!(
            first.evidence["graph_id"], second.evidence["graph_id"],
            "the repeated request must point at the same durable graph"
        );
        assert_eq!(
            objectives.lock().expect("objectives").len(),
            executed_objectives,
            "the duplicate model request must not execute protocol agents again"
        );
    }

    #[tokio::test]
    async fn automatic_team_executes_bounded_graph_and_replays_by_collaboration_lease() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let providers = crate::config::ProvidersConfig {
            providers: HashMap::from([(
                "test".to_string(),
                crate::config::ProviderConfig {
                    name: "test".to_string(),
                    base_url: "https://example.test/v1".to_string(),
                    api_key: "test".to_string(),
                    models: vec!["fast".to_string()],
                    protocol: Some("responses".to_string()),
                },
            )]),
        };
        let services = RuntimeServices::builder(temp.path(), &workspace)
            .provider_registry(Arc::new(
                crate::ProviderRegistry::new(providers).expect("provider registry"),
            ))
            .build()
            .expect("runtime services");
        let objectives = Arc::new(Mutex::new(Vec::new()));
        services
            .agent_runtime()
            .register_backend(Arc::new(CompletedProtocolBackend {
                objectives: Arc::clone(&objectives),
            }));
        let objective = "这是复杂架构审查，必须实际启动一个多 Agent 协作团队，分别审视 crates/runtime、crates/gateway、surfaces/webui 的策略事件接线、权限边界和用户可见状态，再交叉验证并综合证据。";
        let mut decision = crate::execution_core::build_runtime_execution_decision(objective, None);
        assert_eq!(
            decision.strategy.selected_candidate,
            harness_contract::strategy::ExecutionCandidateKind::Team
        );
        decision.session_ref = Some("session-1".to_string());
        decision.turn_ref = Some("turn-auto-team".to_string());
        let binding = harness_contract::team::TeamStrategyBinding {
            decision_id: decision.decision_id.clone(),
            decision_revision: decision.decision_revision,
            decision_lease: decision.lease.lease_id.clone(),
            turn_ref: "turn-auto-team".to_string(),
        };
        let parent = harness_contract::execution_graph::ExecutionParentBinding {
            execution_id: "turn-graph-auto".to_string(),
            node_id: "turn-graph-auto:model".to_string(),
        };
        let mut automatic = request(RuntimeOrchestrationAction::RequestTeam);
        automatic.intent = objective.to_string();
        automatic.model_lease = Some("fast".to_string());
        automatic.selection_mode = Some(harness_contract::team::TeamSelectionMode::Automatic);
        automatic.strategy_binding = Some(binding.clone());
        automatic.constraints.max_parallel_agents = Some(3);
        automatic.constraints.risk =
            Some(format!("{:?}", decision.strategy.understanding.risk).to_ascii_lowercase());
        let scopes = [
            "read:crates/runtime",
            "read:crates/gateway",
            "read:surfaces/webui",
        ];
        automatic.capabilities = scopes
            .iter()
            .map(|scope| format!("resource:{scope}"))
            .collect();
        automatic.focus_partition_plans = vec![
            harness_contract::team::FocusPartitionPlan {
                role_id: "researcher".to_string(),
                shared_baseline: vec!["same parent objective".to_string()],
                slots: scopes
                    .iter()
                    .enumerate()
                    .map(
                        |(index, scope)| harness_contract::team::FocusPartitionSlot {
                            focus_id: format!("focus-{index}"),
                            boundary: format!("inspect only {scope}"),
                            evidence_responsibility: format!("evidence from {scope}"),
                            capability_cropped_refs: vec![(*scope).to_string()],
                            scope_hash: harness_contract::team::focus_scope_hash(
                                "researcher",
                                &format!("inspect only {scope}"),
                                &[(*scope).to_string()],
                            ),
                            overlap_budget_bp: 0,
                            novelty_target_bp: 2_500,
                            output_contract: vec!["findings".to_string(), "evidence".to_string()],
                            output_acceptance: vec![format!(
                                "evidence_scope:{}",
                                scope.trim_start_matches("read:")
                            )],
                        },
                    )
                    .collect(),
            },
            harness_contract::team::FocusPartitionPlan {
                role_id: "synthesizer".to_string(),
                shared_baseline: vec!["bounded researcher outputs only".to_string()],
                slots: vec![harness_contract::team::FocusPartitionSlot {
                    focus_id: "bounded-synthesis".to_string(),
                    boundary: "synthesize only bounded researcher evidence".to_string(),
                    evidence_responsibility: "preserve evidence scope and unresolved gaps"
                        .to_string(),
                    capability_cropped_refs: scopes
                        .iter()
                        .map(|scope| (*scope).to_string())
                        .collect(),
                    scope_hash: harness_contract::team::focus_scope_hash(
                        "synthesizer",
                        "synthesize only bounded researcher evidence",
                        &scopes
                            .iter()
                            .map(|scope| (*scope).to_string())
                            .collect::<Vec<_>>(),
                    ),
                    overlap_budget_bp: 0,
                    novelty_target_bp: 1_000,
                    output_contract: vec![
                        "summary".to_string(),
                        "evidence".to_string(),
                        "unresolved".to_string(),
                    ],
                    output_acceptance: vec!["evidence".to_string(), "unresolved".to_string()],
                }],
            },
        ];

        let (left, right) = tokio::join!(
            submit_runtime_orchestration_request(
                automatic.clone(),
                Some(&decision),
                services.as_ref(),
                Some(parent.clone()),
            ),
            submit_runtime_orchestration_request(
                automatic.clone(),
                Some(&decision),
                services.as_ref(),
                Some(parent.clone()),
            ),
        );
        let (first, concurrent_replay) = if left.execution["type"] == "execution_graph_run" {
            (left, right)
        } else {
            (right, left)
        };
        assert_eq!(first.status, "completed", "{first:?}");
        assert_eq!(
            concurrent_replay.status, "completed",
            "{concurrent_replay:?}"
        );
        assert_eq!(
            concurrent_replay.execution["type"],
            "execution_graph_reused"
        );
        assert_eq!(
            concurrent_replay.evidence["graph_id"],
            first.evidence["graph_id"]
        );
        assert_eq!(first.evidence["executed"], true);
        assert_eq!(first.evidence["working_state_verified"], true);
        let graph_id = first.evidence["graph_id"].as_str().expect("graph id");
        let graph = services
            .graph_state_store()
            .load(graph_id)
            .expect("automatic Team graph");
        let agent_packets = graph
            .nodes
            .iter()
            .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
            .map(|node| serde_json::from_str::<AgentTaskPacket>(&node.payload_ref).unwrap())
            .collect::<Vec<_>>();
        assert!(agent_packets.len() >= 2);
        assert!(agent_packets.iter().all(|packet| {
            packet.budget_lease.max_tokens <= 24_000
                && packet
                    .constraints
                    .contains(&format!("collaboration_lease:{}", binding.decision_lease))
                && packet
                    .constraints
                    .contains(&"nested_team:forbidden".to_string())
                && packet
                    .constraints
                    .contains(&"parent_merge:exactly_once".to_string())
        }));
        let executed_agents = objectives.lock().expect("objectives").len();

        automatic.intent = "same leased turn, rephrased after terminal receipt".to_string();
        // Match the Gateway's model-origin sanitization: the provider cannot
        // self-report a binding or selection mode. Runtime must inject the
        // active lease before reuse lookup.
        automatic.strategy_binding = None;
        automatic.selection_mode = None;
        automatic.focus_partition_plans.clear();
        let replay = submit_runtime_orchestration_request(
            automatic,
            Some(&decision),
            services.as_ref(),
            Some(parent),
        )
        .await;
        assert_eq!(replay.status, "completed", "{replay:?}");
        assert_eq!(replay.execution["type"], "execution_graph_reused");
        assert_eq!(replay.request_id, first.request_id);
        assert_eq!(replay.evidence["request_id"], first.evidence["request_id"]);
        assert_eq!(replay.evidence["graph_id"], first.evidence["graph_id"]);
        assert_eq!(
            objectives.lock().expect("objectives").len(),
            executed_agents
        );
    }

    #[test]
    fn critical_risk_still_requires_global_approval() {
        let execution = crate::execution_core::build_runtime_execution_decision(
            "force push 并 reset --hard 清理所有内容",
            None,
        );
        let mut critical = request(RuntimeOrchestrationAction::PlanOnly);
        critical.constraints.risk = Some("critical".to_string());

        let decision = validator::validate_request(&critical, &execution, None, None);

        assert_eq!(decision.status, "needs_approval");
        assert!(decision
            .policy_gates
            .contains(&harness_contract::core::ExecutionPolicyGate::Risk));
        assert!(decision
            .policy_gates
            .contains(&harness_contract::core::ExecutionPolicyGate::Approval));
        assert!(decision
            .validation_findings
            .contains(&"risk_requires_approval".to_string()));
    }
}
