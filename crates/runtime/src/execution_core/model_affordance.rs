use crate::execution_core::strategy_decision::{
    action_selection_report_for_decision, RuntimeExecutionDecision,
};
use harness_contract::tool::ToolExposureProjection;

#[must_use]
pub fn runtime_execution_guidance_prompt(decision: &RuntimeExecutionDecision) -> String {
    runtime_execution_guidance_prompt_with_tool_exposure(decision, None)
}

/// Render the per-request tool contract that accompanies a runtime execution
/// decision. The provider's native function schema is the only authority for
/// what a model can call on this request; the broader catalog is discovery
/// data, not an invitation to simulate unavailable tools.
#[must_use]
pub fn runtime_execution_guidance_prompt_with_tool_exposure(
    decision: &RuntimeExecutionDecision,
    exposure: Option<&ToolExposureProjection>,
) -> String {
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
    let tool_contract = exposure.map_or_else(
        || {
            "## Current function-call contract\nNo runtime exposure projection is available for this request. Call only native function schemas actually supplied by the provider; catalog names in context are discovery candidates, not callable tools."
                .to_string()
        },
        |exposure| {
            let active = if exposure.active_ids.is_empty() {
                "none".to_string()
            } else {
                exposure.active_ids.join(", ")
            };
            let deferred = if exposure.deferred_ids.is_empty() {
                "none".to_string()
            } else {
                exposure.deferred_ids.join(", ")
            };
            let discovery_instruction = if exposure.active_ids.iter().any(|id| id == "ToolSearch")
                && !exposure.deferred_ids.is_empty()
            {
                "To use a deferred catalog capability, make one focused `ToolSearch` call describing the work. Accepted candidates become native function schemas on the next model request. Do not emit simulated markup or call a deferred name before that activation."
            } else if exposure.deferred_ids.is_empty() {
                "There are no deferred catalog capabilities for this request."
            } else {
                "Deferred catalog capabilities cannot be activated on this request because `ToolSearch` is not an active native function schema. Do not simulate them."
            };
            format!(
                "## Current function-call contract\nOnly these native provider function schemas are callable on this request: [{active}].\nDeferred catalog candidates (not callable yet): [{deferred}].\n{discovery_instruction}\nexposure_revision={}; catalog_revision={}; reason={}",
                exposure.exposure_revision,
                exposure.catalog_revision,
                exposure.reason,
            )
        },
    );
    format!(
        "## Runtime execution decision\nrecommended_pattern={}; evidence_mode={:?}; complexity={:?}; risk={:?}\nrecommended_template={}\nrecommended_actions={}\naction_selection={}\nContract instruction: {}\n{}\nGuidance: simple work should be answered directly. When the right pattern is unclear, use `runtime_capabilities` once for a compact decision. Call `runtime_orchestrate(action=...)` only when it is listed in the current native function-call contract and a real runtime state change is intended. Prefer batched evidence, Tool DAG, ReWOO, TeamRuntime, or deliberation over slow repeated probing. Gateway/API sessions auto-bind session_id, so an active `request_team` call can create a real mission-bound team. If progress is useful but slow, continue and provide staged synthesis; if tool calls repeat with low novelty, switch strategy before spending more budget.",
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
        tool_contract,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        runtime_execution_guidance_prompt, runtime_execution_guidance_prompt_with_tool_exposure,
    };
    use crate::execution_core::build_runtime_execution_decision;
    use harness_contract::tool::ToolExposureProjection;

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

    #[test]
    fn per_request_guidance_distinguishes_active_and_deferred_tools() {
        let decision = build_runtime_execution_decision("并行审查当前代码并给出证据", None);
        let prompt = runtime_execution_guidance_prompt_with_tool_exposure(
            &decision,
            Some(&ToolExposureProjection {
                catalog_revision: 7,
                exposure_revision: 3,
                bootstrap_ids: vec!["ToolSearch".to_string()],
                active_ids: vec!["ToolSearch".to_string(), "runtime_capabilities".to_string()],
                deferred_ids: vec!["read_many".to_string(), "runtime_orchestrate".to_string()],
                fallback_full: false,
                reason: "bootstrap tools exposed".to_string(),
                schema_tokens: 0,
            }),
        );

        assert!(prompt.contains("Only these native provider function schemas"));
        assert!(prompt.contains("Deferred catalog candidates (not callable yet)"));
        assert!(prompt.contains("make one focused `ToolSearch` call"));
        assert!(prompt.contains("Do not emit simulated markup"));
    }
}
