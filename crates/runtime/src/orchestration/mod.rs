//! Runtime-owned orchestration contract.

pub mod executor;
pub mod planner;
pub mod request;
pub mod result;
pub mod validator;

use serde_json::{json, Value};

use crate::tool_host::RuntimeToolExecutionHost;
use crate::{global_mission_evidence_bus, MissionEvidenceRef};

pub use planner::{plan_runtime_collaboration_decision, RuntimeOrchestrationPlan};
pub use request::{
    RuntimeOrchestrationAction, RuntimeOrchestrationConstraints, RuntimeOrchestrationRequest,
};
pub use result::{RuntimeOrchestrationDecision, RuntimeOrchestrationResult};

#[must_use]
pub fn handle_runtime_orchestration_request(
    request: RuntimeOrchestrationRequest,
) -> RuntimeOrchestrationResult {
    handle_runtime_orchestration_request_with_host(request, None)
}

#[must_use]
pub fn handle_runtime_orchestration_request_with_host(
    request: RuntimeOrchestrationRequest,
    tool_host: Option<&dyn RuntimeToolExecutionHost>,
) -> RuntimeOrchestrationResult {
    let plan = planner::plan_runtime_orchestration(&request);
    let mut decision =
        validator::validate_request(&request, &plan.execution_decision.recommended_mode);
    let (status_override, detail) =
        executor::execute_orchestration_request(&request, &plan, &decision.status, tool_host);
    if let Some(status) = status_override {
        decision.status = status;
    }
    let request_id = format!("runtime-orch-{}", uuid::Uuid::new_v4());
    let evidence = orchestration_evidence(&request_id, &request, &plan, &decision);
    RuntimeOrchestrationResult {
        request_id,
        status: decision.status.clone(),
        decision,
        execution: detail,
        evidence,
        next_model_guidance: executor::guidance_for(&request.action),
    }
}

#[must_use]
pub fn runtime_orchestration_response(value: Value) -> Value {
    runtime_orchestration_response_with_host(value, None)
}

#[must_use]
pub fn runtime_orchestration_response_with_host(
    value: Value,
    tool_host: Option<&dyn RuntimeToolExecutionHost>,
) -> Value {
    match serde_json::from_value::<RuntimeOrchestrationRequest>(value) {
        Ok(request) => serde_json::to_value(handle_runtime_orchestration_request_with_host(request, tool_host))
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

fn orchestration_evidence(
    request_id: &str,
    request: &RuntimeOrchestrationRequest,
    plan: &RuntimeOrchestrationPlan,
    decision: &RuntimeOrchestrationDecision,
) -> Value {
    let mut evidence = json!({
        "type": "runtime_orchestration_evidence",
        "request_id": request_id,
        "runtime_action": runtime_action_alias(request.action),
        "tool_action": request.action,
        "status": &decision.status,
        "model_intent": &request.intent,
        "model_reason": &request.reason,
        "template_hint": &request.template_hint,
        "selected_mode": decision.selected_mode.as_str(),
        "recommended_template": plan.execution_decision.recommended_template.map(|template| template.as_str()),
        "policy_gates": &decision.policy_gates,
        "accepted": matches!(decision.status.as_str(), "accepted" | "running" | "ready" | "planned" | "executed"),
        "degraded": !matches!(decision.status.as_str(), "accepted" | "running" | "ready" | "planned" | "executed"),
        "evidence_refs": &request.evidence_refs,
        "runtime_owner": "runtime.orchestration",
    });
    if let Some(recorded) = record_orchestration_evidence(request_id, request, decision) {
        if let Some(object) = evidence.as_object_mut() {
            object.insert("mission_evidence".to_string(), json!(recorded));
        }
    }
    evidence
}

fn record_orchestration_evidence(
    request_id: &str,
    request: &RuntimeOrchestrationRequest,
    decision: &RuntimeOrchestrationDecision,
) -> Option<MissionEvidenceRef> {
    let session_id = request
        .session_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())?;
    Some(global_mission_evidence_bus().record(MissionEvidenceRef {
        evidence_id: String::new(),
        mission_id: None,
        session_id: session_id.to_string(),
        team_id: None,
        agent_id: None,
        kind: "runtime_orchestration".to_string(),
        summary: format!(
            "runtime action `{}` resolved as `{}`",
            runtime_action_alias(request.action),
            decision.status
        ),
        source_ref: Some(request_id.to_string()),
        created_at_ms: 0,
    }))
}

fn runtime_action_alias(action: RuntimeOrchestrationAction) -> &'static str {
    match action {
        RuntimeOrchestrationAction::PlanOnly => "continue_single",
        RuntimeOrchestrationAction::RequestTeam => "use_team_template",
        RuntimeOrchestrationAction::RequestSubagent
        | RuntimeOrchestrationAction::RequestVerification
        | RuntimeOrchestrationAction::RequestBackgroundReview => "use_team_template",
        RuntimeOrchestrationAction::RequestParallelTools => "parallel_tool_batch",
        RuntimeOrchestrationAction::RequestRewooEvidence => "parallel_tool_batch",
        RuntimeOrchestrationAction::RequestDeliberation => "use_team_template",
        RuntimeOrchestrationAction::RequestReflexionRetry => "request_arbiter",
        RuntimeOrchestrationAction::RequestRiskGate => "ask_approval",
        RuntimeOrchestrationAction::RequestSessionLink => "dispatch_session",
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
        assert_eq!(result.evidence["runtime_action"], "continue_single");
        assert_eq!(result.evidence["tool_action"], "plan_only");
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
        assert_eq!(result.evidence["runtime_action"], "use_team_template");
        assert_eq!(result.evidence["accepted"], false);
        assert!(result
            .decision
            .policy_gates
            .contains(&"missing_session_id_for_team_runtime".to_string()));
    }

    struct FakeRuntimeToolHost;

    impl crate::RuntimeToolExecutionHost for FakeRuntimeToolHost {
        fn execute_runtime_tool(
            &self,
            request: &crate::RuntimeToolExecutionRequest,
        ) -> crate::RuntimeToolExecutionOutcome {
            crate::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: crate::RuntimeToolExecutionStatus::Executed,
                category: request.category,
                output: Some(format!("executed {}", request.tool_name)),
                error: None,
                evidence_ref: format!("fake-evidence:{}", request.tool_use_id),
            }
        }
    }

    #[test]
    fn tool_dag_actions_without_host_are_blocked_not_fake_ready() {
        for action in [
            RuntimeOrchestrationAction::RequestParallelTools,
            RuntimeOrchestrationAction::RequestRewooEvidence,
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
            assert_eq!(result.status, "blocked_missing_executor");
            assert_eq!(
                result.execution["receipt"]["status"],
                "blocked_missing_executor"
            );
            assert_eq!(
                result.execution["engine"]["dispatch_surface"],
                "runtime_tool_host"
            );
        }
    }

    #[test]
    fn tool_dag_actions_with_host_execute_and_emit_receipt() {
        let host = FakeRuntimeToolHost;
        for action in [
            RuntimeOrchestrationAction::RequestParallelTools,
            RuntimeOrchestrationAction::RequestRewooEvidence,
        ] {
            let result = handle_runtime_orchestration_request_with_host(
                RuntimeOrchestrationRequest {
                    intent: "检查 README 是否反映最新架构".to_string(),
                    session_id: Some("session-runtime-orchestrate-test".to_string()),
                    action,
                    reason: None,
                    template_hint: None,
                    capabilities: Vec::new(),
                    evidence_refs: Vec::new(),
                    constraints: Default::default(),
                    surface: None,
                },
                Some(&host),
            );
            assert_eq!(result.status, "executed");
            assert_eq!(result.execution["receipt"]["status"], "executed");
            assert!(result.execution["receipt"]["tool_results"]
                .as_array()
                .is_some_and(|items| !items.is_empty()));
            assert_eq!(
                result.execution["receipt"]["events"][0]["kind"],
                "runtime.tool_dag.executed"
            );
            assert!(result.evidence["accepted"].as_bool().unwrap_or(false));
        }
    }

    #[test]
    fn deliberation_and_reflexion_return_runtime_owned_ready_packets() {
        for action in [
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
            assert!(result.execution["engine"]["owned_by"] == "runtime");
        }
    }

    #[test]
    fn runtime_orchestrate_request_subagent_starts_agent_lifecycle() {
        let session_id = format!("session-subagent-{}", uuid::Uuid::new_v4());
        let _ = crate::global_mission_runtime().start_session(crate::StartMissionSessionRequest {
            title: "subagent orchestration test".to_string(),
            session_id: Some(session_id.clone()),
        });
        let result = handle_runtime_orchestration_request(RuntimeOrchestrationRequest {
            intent: "分析 Runtime 编排链路".to_string(),
            session_id: Some(session_id),
            action: RuntimeOrchestrationAction::RequestSubagent,
            reason: None,
            template_hint: Some("explorer".to_string()),
            capabilities: Vec::new(),
            evidence_refs: vec!["evidence:runtime".to_string()],
            constraints: Default::default(),
            surface: None,
        });

        assert!(matches!(
            result.status.as_str(),
            "running" | "running_degraded"
        ));
        assert_eq!(result.execution["type"], "runtime_orchestration_result");
        assert_eq!(
            result.execution["execution_fidelity"],
            "runtime_owned_agent_lifecycle"
        );
        assert!(result.execution["agent_id"].is_string());
        assert!(result.execution["event_refs"]
            .as_array()
            .is_some_and(|items| !items.is_empty()));
        let obsolete_gate = ["planning_only_until", "_runtime_context_attached"].concat();
        assert!(!result.decision.policy_gates.contains(&obsolete_gate));
    }

    #[test]
    fn runtime_orchestrate_request_verification_starts_verifier() {
        let result = handle_runtime_orchestration_request(RuntimeOrchestrationRequest {
            intent: "验证 V15 是否真的接线".to_string(),
            session_id: None,
            action: RuntimeOrchestrationAction::RequestVerification,
            reason: None,
            template_hint: None,
            capabilities: Vec::new(),
            evidence_refs: vec!["plan:v15".to_string()],
            constraints: Default::default(),
            surface: None,
        });

        assert_eq!(result.status, "running");
        assert_eq!(
            result.execution["execution_fidelity"],
            "runtime_owned_verification_lifecycle"
        );
        assert_eq!(result.execution["agent"]["subagentType"], "Verification");
    }

    #[test]
    fn runtime_orchestrate_background_review_starts_steward() {
        let result = handle_runtime_orchestration_request(RuntimeOrchestrationRequest {
            intent: "持续监督复杂重构风险".to_string(),
            session_id: Some(format!("session-review-{}", uuid::Uuid::new_v4())),
            action: RuntimeOrchestrationAction::RequestBackgroundReview,
            reason: None,
            template_hint: None,
            capabilities: Vec::new(),
            evidence_refs: vec!["risk:architecture".to_string()],
            constraints: Default::default(),
            surface: None,
        });

        assert_eq!(result.status, "running");
        assert_eq!(
            result.execution["execution_fidelity"],
            "runtime_owned_steward_lifecycle"
        );
        assert!(result.execution["steward_id"].is_string());
        assert!(result.execution["control_actions"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| item == "pause")));
    }

    #[test]
    fn runtime_orchestrate_session_link_generic_path_routes_command() {
        let from_session = format!("session-link-from-{}", uuid::Uuid::new_v4());
        let target_session = format!("session-link-target-{}", uuid::Uuid::new_v4());
        let _ = crate::global_mission_runtime().start_session(crate::StartMissionSessionRequest {
            title: "source session".to_string(),
            session_id: Some(from_session.clone()),
        });
        let _ = crate::global_mission_runtime().start_session(crate::StartMissionSessionRequest {
            title: "target session".to_string(),
            session_id: Some(target_session.clone()),
        });
        let result = handle_runtime_orchestration_request(RuntimeOrchestrationRequest {
            intent: "同步检查结果".to_string(),
            session_id: Some(from_session),
            action: RuntimeOrchestrationAction::RequestSessionLink,
            reason: None,
            template_hint: Some(target_session),
            capabilities: Vec::new(),
            evidence_refs: Vec::new(),
            constraints: Default::default(),
            surface: None,
        });

        assert!(matches!(result.status.as_str(), "routed" | "rejected"));
        assert_eq!(
            result.execution["execution_fidelity"],
            "runtime_owned_session_bridge"
        );
        assert_eq!(
            result.execution["bridge"]["kind"],
            "runtime.cross_session_bridge_receipt"
        );
    }

    #[test]
    fn strategy_orchestration_records_model_visible_evidence() {
        let result = handle_runtime_orchestration_request(RuntimeOrchestrationRequest {
            intent: "复杂代码审计需要并行证据和团队审查".to_string(),
            session_id: Some("session-strategy-orchestration-evidence".to_string()),
            action: RuntimeOrchestrationAction::RequestParallelTools,
            reason: Some("independent files can be inspected together".to_string()),
            template_hint: Some("fanout_research_synthesis".to_string()),
            capabilities: vec!["read".to_string(), "search".to_string()],
            evidence_refs: vec!["source:readme".to_string()],
            constraints: Default::default(),
            surface: Some("webui".to_string()),
        });

        assert_eq!(result.status, "blocked_missing_executor");
        assert_eq!(result.evidence["runtime_action"], "parallel_tool_batch");
        assert_eq!(
            result.evidence["model_reason"],
            "independent files can be inspected together"
        );
        assert_eq!(
            result.evidence["template_hint"],
            "fanout_research_synthesis"
        );
        assert_eq!(
            result.evidence["mission_evidence"]["kind"],
            "runtime_orchestration"
        );
        assert_eq!(result.execution["engine"]["owned_by"], "runtime");
        assert_eq!(
            result.execution["receipt"]["status"],
            "blocked_missing_executor"
        );
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
        assert_eq!(result.evidence["runtime_action"], "use_team_template");
        assert_eq!(
            result.evidence["mission_evidence"]["kind"],
            "runtime_orchestration"
        );
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
