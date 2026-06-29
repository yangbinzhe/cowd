#[must_use]
pub fn runtime_orchestration_actions() -> Vec<&'static str> {
    vec![
        "plan_only",
        "request_team",
        "request_subagent",
        "request_verification",
        "request_parallel_tools",
        "request_rewoo_evidence",
        "request_deliberation",
        "request_reflexion_retry",
        "request_background_review",
        "request_risk_gate",
        "request_session_link",
    ]
}

#[must_use]
pub fn runtime_orchestration_action_guidance() -> &'static str {
    "Use runtime_orchestrate for controlled runtime planning/execution; gateway/API sessions auto-bind session_id for real TeamRuntime requests, while detached/offline calls must pass session_id explicitly."
}
