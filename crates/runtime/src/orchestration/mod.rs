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
    let Some(compiled) = compiled else {
        return result;
    };
    if !should_execute(&result, &compiled) {
        return result;
    }

    // A model can legitimately inspect the completed receipt and mention the
    // same team request again in a later model step. A second protocol graph
    // would repeat the work and spend another provider budget. Graph lineage
    // is the durable source of truth, so reuse is decided here rather than in
    // a Gateway adapter or an in-memory conversation cache.
    if let Some(reused) =
        find_reusable_team_execution(&reuse_request, &compiled, services, reuse_parent.as_ref())
            .await
    {
        return apply_reused_team_execution(result, reused);
    }

    let graph = match services.compile_graph_agent_intents(compiled.graph) {
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
    };
    let graph_id = graph.id.clone();
    let compiled_team = declares_team(&graph);
    match services.graph_runner().start(graph).await {
        Ok(report) => match services.graph_runner().projection(&graph_id).await {
            Ok(projection) => {
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
                result.evidence["graph_id"] = Value::String(graph_id);
                if compiled_team && result.status == "completed" {
                    result.next_model_guidance = "The requested team completed in this turn. Treat the terminal summary and durable graph evidence as the canonical result; do not create another overlapping team or deliberation graph unless the user introduces a genuinely independent objective."
                        .to_string();
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
        }
    }
    result
}

#[derive(Debug)]
struct ReusedTeamExecution {
    graph_id: String,
    projection: ExecutionGraphProjection,
}

async fn find_reusable_team_execution(
    request: &RuntimeOrchestrationRequest,
    compiled: &CompiledOrchestration,
    services: &RuntimeServices,
    parent: Option<&harness_contract::execution_graph::ExecutionParentBinding>,
) -> Option<ReusedTeamExecution> {
    if !declares_team(&compiled.graph) {
        return None;
    }
    let parent = parent?;
    let links = services
        .graph_state_store()
        .child_links_async(parent.execution_id.clone())
        .await
        .ok()?;
    for link in links {
        if !same_team_objective(&link.child_objective, &request.intent) {
            continue;
        }
        let graph = services
            .graph_state_store()
            .load_async(link.child_execution_id.clone())
            .await
            .ok()?;
        if !declares_team(&graph) || !same_team_objective(&graph.objective, &request.intent) {
            continue;
        }
        let projection = services
            .graph_state_store()
            .projection_async(link.child_execution_id.clone())
            .await
            .ok()?;
        return Some(ReusedTeamExecution {
            graph_id: link.child_execution_id,
            projection,
        });
    }
    None
}

fn apply_reused_team_execution(
    mut result: RuntimeOrchestrationResult,
    reused: ReusedTeamExecution,
) -> RuntimeOrchestrationResult {
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
        "reused_from_graph_id": reused.graph_id,
    });
    result.evidence["accepted"] = Value::Bool(result.status == "completed");
    result.evidence["executed"] = Value::Bool(true);
    result.evidence["reused"] = Value::Bool(true);
    result.evidence["graph_id"] = Value::String(reused.graph_id);
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
    request: RuntimeOrchestrationRequest,
    leased_decision: Option<&RuntimeExecutionDecision>,
    services: Option<&RuntimeServices>,
    parent_execution: Option<harness_contract::execution_graph::ExecutionParentBinding>,
) -> Result<(RuntimeOrchestrationResult, Option<CompiledOrchestration>), RuntimeOrchestrationResult>
{
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
                outcome: "protocol agent completed with evidence-aware output".to_string(),
                acceptance: vec!["completed".to_string()],
                evidence_refs: Vec::new(),
                changes: Vec::new(),
                conflicts: Vec::new(),
                unresolved: Vec::new(),
                input_tokens: 11,
                output_tokens: 7,
                model: selection.model,
                provider: selection.provider,
                tool_calls: 0,
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
            reason: None,
            template_hint: None,
            focus_partition_plans: Vec::new(),
            capabilities: Vec::new(),
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
}
