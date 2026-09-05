#[must_use]
pub fn runtime_orchestration_actions() -> Vec<&'static str> {
    harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.to_vec()
}

#[must_use]
pub fn runtime_orchestration_action_guidance() -> &'static str {
    "Use small Agent actions incrementally. The model owns Team purpose, roles, Tasks, dependencies, discussion, challenge and replanning; Runtime owns actor binding, permissions, revisions, leases, concurrent execution, recovery and terminal verification. Keep long content in ordinary output or durable files and pass compact references through actions."
}
