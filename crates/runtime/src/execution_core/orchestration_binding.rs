#[must_use]
pub fn runtime_orchestration_actions() -> Vec<&'static str> {
    vec!["inspect", "propose", "revise", "control"]
}

#[must_use]
pub fn runtime_orchestration_action_guidance() -> &'static str {
    "Use runtime_orchestrate(operation=inspect) for a bounded state snapshot. For state changes, propose or revise only semantic recipes (agent, team, review, synthesis, session_dispatch) and dependencies; Runtime resolves executors, definitions, leases, approvals and physical graph identities. Use control with an exact graph revision for pause, resume, or scoped cancellation."
}
