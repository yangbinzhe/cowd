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

use crate::execution_core::RuntimeExecutionDecision;
use crate::RuntimeServices;
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
    compile_runtime_orchestration_request(request, None, None)
}

#[must_use]
pub fn handle_runtime_orchestration_request_with_decision(
    request: RuntimeOrchestrationRequest,
    leased_decision: Option<&RuntimeExecutionDecision>,
) -> RuntimeOrchestrationResult {
    compile_runtime_orchestration_request(request, leased_decision, None)
}

/// Compile against workspace policy. Execution is owned by the active session
/// runtime, which binds provider, tool, agent, and session backends to its graph.
pub async fn submit_runtime_orchestration_request(
    request: RuntimeOrchestrationRequest,
    leased_decision: Option<&RuntimeExecutionDecision>,
    services: &RuntimeServices,
) -> RuntimeOrchestrationResult {
    let (mut result, compiled) =
        match compile_runtime_orchestration(request, leased_decision, Some(services)) {
            Ok(compiled) => compiled,
            Err(result) => return result,
        };
    let Some(compiled) = compiled else {
        return result;
    };
    if !should_execute(&result, &compiled) {
        return result;
    }

    let graph_id = compiled.graph.id.clone();
    match services.graph_runner().start(compiled.graph).await {
        Ok(report) => match services.graph_runner().projection(&graph_id).await {
            Ok(projection) => {
                let terminal = projection.terminal_result_ref.clone();
                result.status = if report.failed > 0 || report.blocked > 0 {
                    "blocked".to_string()
                } else if report.waiting > 0 {
                    "waiting".to_string()
                } else {
                    "completed".to_string()
                };
                result.decision.status = result.status.clone();
                result.execution = json!({
                    "type": "execution_graph_run",
                    "status": result.status,
                    "protocol": compiled.protocol,
                    "report": report,
                    "projection": projection,
                    "terminal_result_ref": terminal,
                });
                result.evidence["accepted"] = Value::Bool(true);
                result.evidence["executed"] = Value::Bool(true);
                result.evidence["graph_id"] = Value::String(graph_id);
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

fn should_execute(result: &RuntimeOrchestrationResult, compiled: &CompiledOrchestration) -> bool {
    result.status == "compiled"
        && compiled.protocol.is_some()
        && result.decision.status == "compiled"
}

fn compile_runtime_orchestration_request(
    request: RuntimeOrchestrationRequest,
    leased_decision: Option<&RuntimeExecutionDecision>,
    services: Option<&RuntimeServices>,
) -> RuntimeOrchestrationResult {
    compile_runtime_orchestration(request, leased_decision, services)
        .map(|(result, _)| result)
        .unwrap_or_else(|result| result)
}

fn compile_runtime_orchestration(
    request: RuntimeOrchestrationRequest,
    leased_decision: Option<&RuntimeExecutionDecision>,
    services: Option<&RuntimeServices>,
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
        match compiler::compile_orchestration(&request_id, &request, &plan) {
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
    decision.selected_protocol = compiled
        .as_ref()
        .and_then(|compiled| compiled.protocol.clone());
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
                "protocol": compiled.protocol,
            })
        },
    );
    let evidence = orchestration_evidence(&request_id, &request, &plan, &decision, compiled_ok);
    Ok((
        RuntimeOrchestrationResult {
            request_id,
            status,
            protocol: compiled
                .as_ref()
                .and_then(|compiled| compiled.protocol.clone()),
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
            session_id: Some("session-1".to_string()),
            action,
            reason: None,
            template_hint: None,
            protocol: None,
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
    async fn deliberation_runs_the_protocol_graph_and_returns_one_terminal_result() {
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
        )
        .await;

        assert_eq!(result.status, "completed", "{result:?}");
        assert_eq!(
            result.protocol.as_ref().map(|item| item.id),
            Some(crate::execution_core::ProtocolId::Debate)
        );
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
}
