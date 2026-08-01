#[must_use]
pub fn runtime_orchestration_actions() -> Vec<&'static str> {
    vec!["inspect", "propose", "revise", "control"]
}

#[must_use]
pub fn runtime_orchestration_action_guidance() -> &'static str {
    "Use runtime_orchestrate(operation=inspect) for a bounded state snapshot. For state changes, propose or revise only semantic recipes (agent, team, review, synthesis, session_dispatch) and dependencies; Runtime resolves executors, definitions, leases, approvals and physical graph identities. Independent evidence lanes can run in parallel. Mark only cancellable read-only lanes required=false, give them a cancellation_group, and let the consumer use dependency any or quorum with cancel_remaining when the task can finish from verified partial coverage. Required writes and synthesis must remain required. Use control with an exact graph revision for pause, resume, or scoped cancellation."
}
