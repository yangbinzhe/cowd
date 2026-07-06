use crate::execution_core::strategy_matcher::{
    build_runtime_action_selection_report, RuntimeExecutionDecision,
};

#[must_use]
pub fn runtime_execution_guidance_prompt(decision: &RuntimeExecutionDecision) -> String {
    format!(
        "## Runtime execution decision\nrecommended_mode={}; evidence_mode={:?}; complexity={:?}; risk={:?}\nrecommended_template={}\nrecommended_actions={}\naction_selection={}\nGuidance: simple work should be answered directly. Complex work should first inspect `runtime_capabilities` when the right mode is unclear, then call `runtime_orchestrate(action=...)` when a real runtime state change is intended. Prefer batched evidence, Tool DAG, ReWOO, TeamRuntime, or deliberation over slow repeated probing. Gateway/API sessions auto-bind session_id, so `request_team` can create a real mission-bound team when the gateway adapter is available. If progress is useful but slow, continue and provide staged synthesis; if tool calls repeat with low novelty, switch strategy before spending more budget.",
        decision.recommended_mode.as_str(),
        decision.evidence_mode,
        decision.complexity,
        decision.risk,
        decision
            .recommended_template
            .map(|template| template.as_str().to_string())
            .unwrap_or_else(|| "none".to_string()),
        serde_json::to_string(&decision.recommended_actions).unwrap_or_else(|_| "[]".to_string()),
        serde_json::to_string(&build_runtime_action_selection_report(
            &decision.user_intent_preview,
            None
        ))
        .unwrap_or_else(|_| "{}".to_string())
    )
}
