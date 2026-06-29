//! Evidence acquisition planning for runtime turns.
//!
//! This module owns the semantic decision of *how* a task should gather
//! evidence. Concrete tools still live in the tools crate and execution remains
//! in the conversation runtime.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::context_fanout::{plan_context_fanout, FanoutToolCall};
use crate::intent_planner::{classify_intent, TaskIntent};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceAcquisitionMode {
    SmallEvidence,
    MediumEvidence,
    LargeEvidence,
    ComplexEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidencePlan {
    pub intent: TaskIntent,
    pub mode: EvidenceAcquisitionMode,
    pub recommended_calls: Vec<FanoutToolCall>,
    pub avoid_patterns: Vec<String>,
    pub use_subagents_when: Vec<String>,
    pub reason: String,
}

#[must_use]
pub fn plan_evidence(prompt: &str) -> EvidencePlan {
    let classified = classify_intent(prompt);
    let mode = acquisition_mode(prompt, classified.intent);
    let recommended_calls = if is_mixed_doc_engineering_review(prompt, classified.intent) {
        merge_calls(readme_review_calls(), plan_context_fanout(prompt).calls)
    } else if is_readme_or_doc_review(prompt) {
        readme_review_calls()
    } else {
        plan_context_fanout(prompt).calls
    };
    let avoid_patterns = avoid_patterns_for(mode, prompt);
    let use_subagents_when = subagent_guidance_for(mode, classified.intent);

    EvidencePlan {
        intent: classified.intent,
        mode,
        recommended_calls,
        avoid_patterns,
        use_subagents_when,
        reason: format!("{}; evidence_mode={mode:?}", classified.reason),
    }
}

#[must_use]
pub fn evidence_plan_prompt(plan: &EvidencePlan) -> String {
    let recommended = if plan.recommended_calls.is_empty() {
        "- no specific initial tool fanout; answer directly if evidence is already sufficient"
            .to_string()
    } else {
        plan.recommended_calls
            .iter()
            .take(6)
            .map(|call| format!("- `{}` with {}", call.name, compact_json(&call.input)))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let avoid = plan
        .avoid_patterns
        .iter()
        .take(4)
        .map(|item| format!("- {item}"))
        .collect::<Vec<_>>()
        .join("\n");
    let delegation = plan
        .use_subagents_when
        .iter()
        .take(4)
        .map(|item| format!("- {item}"))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "## Runtime evidence plan\nmode={:?}; intent={:?}\nreason={}\nRecommended evidence path:\n{}\nAvoid:\n{}\nDelegation/team guidance:\n{}",
        plan.mode, plan.intent, plan.reason, recommended, avoid, delegation
    )
}

fn compact_json(value: &Value) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|_| "{}".to_string())
        .chars()
        .take(360)
        .collect()
}

fn acquisition_mode(prompt: &str, intent: TaskIntent) -> EvidenceAcquisitionMode {
    let lower = prompt.to_lowercase();
    let chars = prompt.chars().count();
    if is_mixed_doc_engineering_review(prompt, intent) {
        return EvidenceAcquisitionMode::ComplexEvidence;
    }
    if is_readme_or_doc_review(prompt) {
        return EvidenceAcquisitionMode::SmallEvidence;
    }
    if contains_any(
        &lower,
        &[
            "team",
            "multi-agent",
            "subagent",
            "parallel",
            "architecture",
            "全盘",
            "多agent",
            "协同",
            "架构",
            "沉浸式",
        ],
    ) || matches!(
        intent,
        TaskIntent::Backend | TaskIntent::Bugfix | TaskIntent::Review
    ) && chars >= 180
    {
        return EvidenceAcquisitionMode::ComplexEvidence;
    }
    if contains_any(
        &lower,
        &[
            "large",
            "全量",
            "全范围",
            "全部",
            "仓库",
            "workspace",
            "deep",
            "深度",
        ],
    ) {
        return EvidenceAcquisitionMode::LargeEvidence;
    }
    if matches!(intent, TaskIntent::Docs | TaskIntent::Review) {
        return EvidenceAcquisitionMode::SmallEvidence;
    }
    EvidenceAcquisitionMode::MediumEvidence
}

fn readme_review_calls() -> Vec<FanoutToolCall> {
    vec![
        call(
            "workspace_snapshot",
            json!({"include_git": true, "include_files": true, "max_files": 300}),
        ),
        call(
            "tool_batch_readonly",
            json!({
                "calls": [
                    {"name": "read_file", "input": {"path": "README.md"}},
                    {"name": "grep_search", "input": {"pattern": "runtime|gateway|surface|tui|memory|matrix|skill|tool|session", "path": "README.md", "-i": true, "head_limit": 80}},
                    {"name": "glob_search", "input": {"pattern": "**/Cargo.toml"}}
                ],
                "max_concurrency": 8
            }),
        ),
    ]
}

fn call(name: &str, input: Value) -> FanoutToolCall {
    FanoutToolCall {
        name: name.to_string(),
        input,
    }
}

fn merge_calls(
    primary: Vec<FanoutToolCall>,
    secondary: Vec<FanoutToolCall>,
) -> Vec<FanoutToolCall> {
    let mut merged = primary;
    for call in secondary {
        let duplicate = merged
            .iter()
            .any(|existing| existing.name == call.name && existing.input == call.input);
        if !duplicate {
            merged.push(call);
        }
    }
    merged
}

fn avoid_patterns_for(mode: EvidenceAcquisitionMode, prompt: &str) -> Vec<String> {
    let mut patterns = vec![
        "Do not repeatedly read overlapping ranges of the same small file.".to_string(),
        "Do not use execute_code to slice a text file when read_many or grep_many can gather the evidence.".to_string(),
        "Do not keep collecting evidence after the available evidence is enough for a staged answer.".to_string(),
    ];
    if is_readme_or_doc_review(prompt) || mode == EvidenceAcquisitionMode::SmallEvidence {
        patterns.push(
            "For README/docs review, prefer full read or batch read plus diff-oriented checks before any range-by-range reading.".to_string(),
        );
    }
    patterns
}

fn subagent_guidance_for(mode: EvidenceAcquisitionMode, intent: TaskIntent) -> Vec<String> {
    if matches!(mode, EvidenceAcquisitionMode::ComplexEvidence)
        || matches!(intent, TaskIntent::Backend | TaskIntent::Bugfix)
    {
        vec![
            "Shape the work for an Explore subagent when independent source areas can be inspected in parallel; this may require runtime-owned orchestration rather than a direct provider tool.".to_string(),
            "Ask for or prepare a Verification subagent when the main agent has enough evidence but needs an independent check.".to_string(),
            "Use or request team/collaboration mode when the task spans architecture, implementation, and validation at the same time.".to_string(),
        ]
    } else {
        vec![
            "Keep the task in the main agent unless evidence domains are independent enough to benefit from parallel review.".to_string(),
        ]
    }
}

fn is_readme_or_doc_review(prompt: &str) -> bool {
    let lower = prompt.to_lowercase();
    contains_any(
        &lower,
        &[
            "readme",
            "docs",
            "documentation",
            "文档",
            "说明",
            "最新的readme",
        ],
    )
}

fn is_mixed_doc_engineering_review(prompt: &str, intent: TaskIntent) -> bool {
    if !is_readme_or_doc_review(prompt) {
        return false;
    }
    let lower = prompt.to_lowercase();
    let architecture_consistency_review = contains_any(
        &lower,
        &[
            "架构是否一致",
            "架构一致",
            "与架构",
            "和架构",
            "architecture",
            "architectural",
        ],
    );
    contains_any(
        &lower,
        &[
            "代码",
            "源码",
            "调用链",
            "全链路",
            "实现",
            "接线",
            "审计",
            "crate",
            "module",
            "runtime",
            "gateway",
            "tools",
            "provider",
            "conversation",
            "scheduler",
            "source",
            "call chain",
        ],
    ) || architecture_consistency_review
        || matches!(intent, TaskIntent::Backend | TaskIntent::Bugfix)
}

fn contains_any(value: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| value.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readme_review_prefers_small_batch_evidence() {
        let plan = plan_evidence("我已经更新，看是否最新的readme还有问题");

        assert_eq!(plan.mode, EvidenceAcquisitionMode::SmallEvidence);
        assert!(plan
            .recommended_calls
            .iter()
            .any(|call| call.name == "tool_batch_readonly"));
        assert!(plan
            .avoid_patterns
            .iter()
            .any(|item| item.contains("README/docs")));
    }

    #[test]
    fn complex_architecture_task_recommends_subagents() {
        let plan =
            plan_evidence("全盘分析 runtime gateway surface 架构并行协同能力，给出深度实施方案");

        assert_eq!(plan.mode, EvidenceAcquisitionMode::ComplexEvidence);
        assert!(plan
            .use_subagents_when
            .iter()
            .any(|item| item.contains("subagent") || item.contains("team")));
    }

    #[test]
    fn mixed_readme_code_architecture_review_keeps_engineering_fanout() {
        let plan = plan_evidence("审查 README、代码架构调用链和 runtime supervisor 接线是否一致");

        assert_eq!(plan.mode, EvidenceAcquisitionMode::ComplexEvidence);
        assert!(plan
            .recommended_calls
            .iter()
            .any(|call| call.name == "tool_batch_readonly"));
        assert!(plan
            .recommended_calls
            .iter()
            .any(|call| call.name == "grep_many" || call.name == "workspace_snapshot"));
        assert!(plan
            .use_subagents_when
            .iter()
            .any(|item| item.contains("subagent") || item.contains("team")));
    }

    #[test]
    fn readme_architecture_consistency_review_keeps_engineering_fanout() {
        let plan = plan_evidence("审查 README 与架构是否一致");

        assert_eq!(plan.mode, EvidenceAcquisitionMode::ComplexEvidence);
        assert!(plan
            .recommended_calls
            .iter()
            .any(|call| call.name == "tool_batch_readonly"));
        assert!(plan
            .recommended_calls
            .iter()
            .any(|call| call.name == "grep_many" || call.name == "workspace_snapshot"));
    }

    #[test]
    fn evidence_plan_prompt_is_model_actionable() {
        let plan = plan_evidence("检查 README 是否反映最新架构");
        assert_eq!(plan.mode, EvidenceAcquisitionMode::SmallEvidence);
        let prompt = evidence_plan_prompt(&plan);

        assert!(prompt.contains("Runtime evidence plan"));
        assert!(prompt.contains("tool_batch_readonly"));
        assert!(prompt.contains("Avoid"));
    }
}
