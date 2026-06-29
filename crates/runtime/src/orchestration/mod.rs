//! Runtime-owned orchestration contract.

pub mod executor;
pub mod planner;
pub mod request;
pub mod result;
pub mod validator;

use serde_json::{json, Value};

pub use planner::{plan_runtime_collaboration_decision, RuntimeOrchestrationPlan};
pub use request::{RuntimeOrchestrationAction, RuntimeOrchestrationRequest};
pub use result::{RuntimeOrchestrationDecision, RuntimeOrchestrationResult};

#[must_use]
pub fn handle_runtime_orchestration_request(
    request: RuntimeOrchestrationRequest,
) -> RuntimeOrchestrationResult {
    let plan = planner::plan_runtime_orchestration(&request);
    let mut decision =
        validator::validate_request(&request, &plan.execution_decision.recommended_mode);
    let (status_override, detail) =
        executor::execute_orchestration_request(&request, &plan, &decision.status);
    if let Some(status) = status_override {
        decision.status = status;
    }
    RuntimeOrchestrationResult {
        request_id: format!("runtime-orch-{}", uuid::Uuid::new_v4()),
        status: decision.status.clone(),
        decision,
        execution: detail,
        next_model_guidance: executor::guidance_for(&request.action),
    }
}

#[must_use]
pub fn runtime_orchestration_response(value: Value) -> Value {
    match serde_json::from_value::<RuntimeOrchestrationRequest>(value) {
        Ok(request) => serde_json::to_value(handle_runtime_orchestration_request(request))
            .unwrap_or_else(|error| {
                json!({"type":"runtime_orchestration_result","status":"rejected","error": error.to_string()})
            }),
        Err(error) => json!({
            "type": "runtime_orchestration_result",
            "status": "rejected",
            "error": format!("invalid runtime_orchestrate input: {error}")
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_only_has_no_side_effect_and_returns_candidates() {
        let result = handle_runtime_orchestration_request(RuntimeOrchestrationRequest {
            intent: "检查 README 是否反映最新架构".to_string(),
            session_id: None,
            action: RuntimeOrchestrationAction::PlanOnly,
            reason: None,
            template_hint: None,
            capabilities: Vec::new(),
            evidence_refs: Vec::new(),
            constraints: Default::default(),
            surface: None,
        });
        assert_eq!(result.status, "planned");
        assert_eq!(result.execution["type"], "plan_only");
    }

    #[test]
    fn request_team_without_session_id_is_rejected_not_fake_accepted() {
        let result = handle_runtime_orchestration_request(RuntimeOrchestrationRequest {
            intent: "需要多 Agent 协同审查架构".to_string(),
            session_id: None,
            action: RuntimeOrchestrationAction::RequestTeam,
            reason: None,
            template_hint: None,
            capabilities: Vec::new(),
            evidence_refs: Vec::new(),
            constraints: Default::default(),
            surface: None,
        });
        assert_eq!(result.status, "rejected");
        assert!(result
            .decision
            .policy_gates
            .contains(&"missing_session_id_for_team_runtime".to_string()));
    }

    #[test]
    fn complex_execution_actions_return_runtime_owned_ready_packets() {
        for action in [
            RuntimeOrchestrationAction::RequestParallelTools,
            RuntimeOrchestrationAction::RequestRewooEvidence,
            RuntimeOrchestrationAction::RequestDeliberation,
            RuntimeOrchestrationAction::RequestReflexionRetry,
        ] {
            let result = handle_runtime_orchestration_request(RuntimeOrchestrationRequest {
                intent: "检查 README 是否反映最新架构".to_string(),
                session_id: Some("session-runtime-orchestrate-test".to_string()),
                action,
                reason: None,
                template_hint: None,
                capabilities: Vec::new(),
                evidence_refs: Vec::new(),
                constraints: Default::default(),
                surface: None,
            });
            assert_eq!(result.status, "ready");
            assert_eq!(result.execution["status"], "ready");
            let fidelity = result.execution["execution_fidelity"]
                .as_str()
                .expect("execution fidelity");
            assert!(fidelity.starts_with("runtime_owned_"), "{fidelity}");
            assert!(!fidelity.contains("planned"), "{fidelity}");
            assert!(result.execution["engine"]["owned_by"] == "runtime");
        }
    }

    #[test]
    fn risk_gate_requires_approval_not_execution() {
        let result = handle_runtime_orchestration_request(RuntimeOrchestrationRequest {
            intent: "执行高风险生产操作".to_string(),
            session_id: Some("session-runtime-orchestrate-test".to_string()),
            action: RuntimeOrchestrationAction::RequestRiskGate,
            reason: None,
            template_hint: None,
            capabilities: Vec::new(),
            evidence_refs: Vec::new(),
            constraints: Default::default(),
            surface: None,
        });
        assert_eq!(result.status, "needs_approval");
        assert_eq!(result.execution["type"], "risk_gate");
        assert!(result
            .decision
            .policy_gates
            .contains(&"risk_gate_requested".to_string()));
    }

    #[test]
    fn request_team_with_session_id_starts_real_team_runtime() {
        let result = handle_runtime_orchestration_request(RuntimeOrchestrationRequest {
            intent: "需要多 Agent 协同审查架构".to_string(),
            session_id: Some("session-runtime-orchestrate-test".to_string()),
            action: RuntimeOrchestrationAction::RequestTeam,
            reason: None,
            template_hint: None,
            capabilities: Vec::new(),
            evidence_refs: Vec::new(),
            constraints: Default::default(),
            surface: None,
        });
        assert_eq!(result.status, "running");
        assert_eq!(result.execution["type"], "team_runtime");
        assert!(result.execution["team"]["team_id"].is_string());
        assert_eq!(
            result.execution["collaboration_run"]["kind"],
            "runtime.collaboration_run"
        );
        assert!(result.execution["control_actions"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| item == "cancel")));
        assert!(result.execution["event_refs"]
            .as_array()
            .is_some_and(|items| !items.is_empty()));
    }
}
