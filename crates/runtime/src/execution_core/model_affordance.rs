use crate::execution_core::strategy_decision::{
    action_selection_report_for_decision, RuntimeExecutionDecision,
};

#[must_use]
pub fn runtime_execution_guidance_prompt(decision: &RuntimeExecutionDecision) -> String {
    let contract_instruction = match decision.pattern() {
        harness_contract::core::ExecutionPattern::Explore => {
            "Acceptance requires grounded evidence. Do not claim a file, web, or workspace fact from prose alone: invoke the applicable read-only tool, retain its receipt/evidence ref, then synthesize from that result."
        }
        harness_contract::core::ExecutionPattern::Collaborate => {
            "The task requests real collaboration. Invoke runtime orchestration to start the selected team/template and wait for its graph-backed Agent/Team receipts; a prose role split without a started team does not satisfy the task."
        }
        _ => {
            "Use the selected pattern directly, and escalate only when the retained evidence or task constraints require it."
        }
    };
    format!(
        "## Runtime execution decision\nrecommended_pattern={}; evidence_mode={:?}; complexity={:?}; risk={:?}\nrecommended_template={}\nrecommended_actions={}\naction_selection={}\nContract instruction: {}\nGuidance: simple work should be answered directly. Complex work should first inspect `runtime_capabilities` when the right pattern is unclear, then call `runtime_orchestrate(action=...)` when a real runtime state change is intended. Prefer batched evidence, Tool DAG, ReWOO, TeamRuntime, or deliberation over slow repeated probing. Gateway/API sessions auto-bind session_id, so `request_team` can create a real mission-bound team when the gateway adapter is available. If progress is useful but slow, continue and provide staged synthesis; if tool calls repeat with low novelty, switch strategy before spending more budget.",
        decision.pattern().as_str(),
        decision.evidence_mode,
        decision.complexity(),
        decision.risk(),
        decision
            .recommended_template
            .map(|template| template.as_str().to_string())
            .unwrap_or_else(|| "none".to_string()),
        serde_json::to_string(&decision.recommended_actions).unwrap_or_else(|_| "[]".to_string()),
        serde_json::to_string(&action_selection_report_for_decision(decision, None))
        .unwrap_or_else(|_| "{}".to_string()),
        contract_instruction,
    )
}

#[cfg(test)]
mod tests {
    use super::runtime_execution_guidance_prompt;
    use crate::execution_core::build_runtime_execution_decision;

    #[test]
    fn evidence_seeking_guidance_requires_a_real_tool_receipt() {
        let decision = build_runtime_execution_decision(
            "读取当前工作区的 Cargo.toml，必须通过工具取得证据。",
            None,
        );

        let prompt = runtime_execution_guidance_prompt(&decision);

        assert!(prompt.contains("invoke the applicable read-only tool"));
        assert!(prompt.contains("receipt/evidence ref"));
    }

    #[test]
    fn collaboration_guidance_requires_a_graph_backed_team() {
        let decision =
            build_runtime_execution_decision("必须实际启动协作团队，完成复杂架构审查。", None);

        let prompt = runtime_execution_guidance_prompt(&decision);

        assert!(prompt.contains("start the selected team/template"));
        assert!(prompt.contains("graph-backed Agent/Team receipts"));
    }
}
