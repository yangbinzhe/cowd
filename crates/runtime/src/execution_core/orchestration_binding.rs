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
        "dispatch_session",
    ]
}

#[must_use]
pub fn runtime_orchestration_action_guidance() -> &'static str {
    "Use runtime_capabilities for read-only runtime planning. Use runtime_orchestrate only for controlled stateful runtime orchestration. Executable lifecycle actions create runtime-owned team/subagent/verification/background/session receipts; deliberation/reflexion return strategy packets for the model to continue; risk_gate returns an approval packet."
}
