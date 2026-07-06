use crate::execution_core::strategy_matcher::RuntimeExecutionDecision;

#[must_use]
pub fn runtime_execution_guidance_prompt(decision: &RuntimeExecutionDecision) -> String {
    format!(
        "## Runtime execution decision\nrecommended_mode={}; evidence_mode={:?}; complexity={:?}; risk={:?}\nrecommended_template={}\nrecommended_actions={}\nGuidance: for complex work, use `runtime_capabilities` detail queries for read-only planning and use `runtime_orchestrate(request_*)` only when you intend to ask runtime to mutate orchestration state such as teams, sessions, approvals, or execution packets. Prefer batched evidence and parallel read-only plans when available. Current user instructions override conflicting recalled memory or knowledge rules for this turn. If progress is useful but slow, continue and provide staged synthesis; if tool calls repeat with low novelty, switch strategy before spending more budget.",
        decision.recommended_mode.as_str(),
        decision.evidence_mode,
        decision.complexity,
        decision.risk,
        decision
            .recommended_template
            .map(|template| template.as_str().to_string())
            .unwrap_or_else(|| "none".to_string()),
        serde_json::to_string(&decision.recommended_actions).unwrap_or_else(|_| "[]".to_string())
    )
}
