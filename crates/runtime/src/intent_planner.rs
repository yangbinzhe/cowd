//! Lightweight intent and dependency planning for tool execution.

use ai_strategy::{decide_strategy, StrategyInput, TaskDomain};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use ai_strategy::{StrategyDecision, TaskUnderstanding};

use crate::tool_dispatch::ToolRequest;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskIntent {
    Review,
    Bugfix,
    Frontend,
    Backend,
    Docs,
    Release,
    Test,
    Explore,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntentPlan {
    pub intent: TaskIntent,
    pub recommended_tools: Vec<String>,
    pub reason: String,
}

#[must_use]
pub fn classify_intent(prompt: &str) -> IntentPlan {
    let decision = classify_strategy(prompt);
    let intent = intent_from_domain(decision.understanding.domain);
    let recommended_tools = recommended_tools_for(intent);
    let reason = reason_for(intent, &decision);

    IntentPlan {
        intent,
        recommended_tools,
        reason,
    }
}

#[must_use]
pub fn classify_strategy(prompt: &str) -> StrategyDecision {
    decide_strategy(&StrategyInput::from_prompt(prompt))
}

fn intent_from_domain(domain: TaskDomain) -> TaskIntent {
    match domain {
        TaskDomain::Review => TaskIntent::Review,
        TaskDomain::Bugfix => TaskIntent::Bugfix,
        TaskDomain::Frontend => TaskIntent::Frontend,
        TaskDomain::Backend | TaskDomain::Architecture => TaskIntent::Backend,
        TaskDomain::Docs | TaskDomain::Research => TaskIntent::Docs,
        TaskDomain::Release => TaskIntent::Release,
        TaskDomain::Test => TaskIntent::Test,
        TaskDomain::Explore => TaskIntent::Explore,
    }
}

fn recommended_tools_for(intent: TaskIntent) -> Vec<String> {
    let tools = match intent {
        TaskIntent::Review => vec!["workspace_snapshot", "grep_many", "read_many"],
        TaskIntent::Bugfix => vec!["grep_many", "read_many", "tool_batch_readonly"],
        TaskIntent::Frontend => vec!["workspace_snapshot", "glob_many", "read_many"],
        TaskIntent::Backend => vec!["workspace_snapshot", "grep_many", "read_many"],
        TaskIntent::Docs => vec!["glob_many", "read_many"],
        TaskIntent::Release => vec!["workspace_snapshot", "tool_batch_readonly"],
        TaskIntent::Test => vec!["workspace_snapshot", "grep_many"],
        TaskIntent::Explore => vec!["workspace_snapshot", "grep_many", "read_many"],
    };
    tools.into_iter().map(str::to_string).collect()
}

fn reason_for(intent: TaskIntent, decision: &StrategyDecision) -> String {
    let base = match intent {
        TaskIntent::Review => {
            "review tasks need workspace status, changed files, and targeted reads"
        }
        TaskIntent::Bugfix => "bugfix tasks need symbol search, callers, and failing test context",
        TaskIntent::Frontend => "frontend tasks need component, style, and route context",
        TaskIntent::Backend => "backend tasks need runtime/module search and source context",
        TaskIntent::Docs => "documentation tasks need related document discovery and batch reads",
        TaskIntent::Release => "release tasks need status, tests, build, and release-gate context",
        TaskIntent::Test => "test tasks need test targets and validation commands",
        TaskIntent::Explore => "exploration starts with workspace snapshot and fanout search/read",
    };
    format!(
        "{base}; strategy_mode={}; complexity={:?}; risk={:?}",
        decision.mode.as_str(),
        decision.understanding.complexity,
        decision.understanding.risk
    )
}

#[must_use]
pub fn infer_tool_dependencies(requests: &mut [ToolRequest]) -> usize {
    let mut added = 0usize;
    for current in 0..requests.len() {
        if !requests[current].depends_on.is_empty() {
            continue;
        }
        let current_kind = mutation_kind(&requests[current].tool_name);
        if !current_kind.needs_dependency {
            continue;
        }
        let Some(current_path) = tool_path(&requests[current]) else {
            continue;
        };
        for previous in (0..current).rev() {
            if path_related(tool_path(&requests[previous]).as_deref(), &current_path)
                || requests[previous].tool_name == "mutation_preview"
                || requests[previous].tool_name == "patch_plan"
                || requests[previous].tool_name == "edit_many_preview"
            {
                requests[current]
                    .depends_on
                    .push(requests[previous].tool_use_id.clone());
                added += 1;
                break;
            }
        }
    }
    added
}

struct MutationKind {
    needs_dependency: bool,
}

fn mutation_kind(tool_name: &str) -> MutationKind {
    MutationKind {
        needs_dependency: matches!(
            tool_name,
            "write_file" | "edit_file" | "apply_patch_transaction" | "checkpoint_restore"
        ),
    }
}

fn tool_path(request: &ToolRequest) -> Option<String> {
    let input = serde_json::from_str::<Value>(&request.input).ok()?;
    input
        .get("path")
        .and_then(Value::as_str)
        .map(normalize_path)
        .or_else(|| {
            input
                .get("edits")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .and_then(|item| item.get("path"))
                .and_then(Value::as_str)
                .map(normalize_path)
        })
}

fn normalize_path(path: &str) -> String {
    path.trim().replace('\\', "/")
}

fn path_related(previous: Option<&str>, current: &str) -> bool {
    let Some(previous) = previous else {
        return false;
    };
    previous == current
        || previous == "."
        || current == "."
        || previous.starts_with(&format!("{current}/"))
        || current.starts_with(&format!("{previous}/"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ai_core::ExecutionMode;

    fn request(id: &str, name: &str, input: &str) -> ToolRequest {
        ToolRequest {
            tool_use_id: id.to_string(),
            tool_name: name.to_string(),
            input: input.to_string(),
            depends_on: Vec::new(),
        }
    }

    #[test]
    fn classifies_review_and_bugfix_intents() {
        assert_eq!(
            classify_intent("please review this PR").intent,
            TaskIntent::Review
        );
        assert_eq!(classify_intent("修复这个失败").intent, TaskIntent::Bugfix);
    }

    #[test]
    fn exposes_strategy_decision_for_runtime_callers() {
        let decision = classify_strategy("全面规划 runtime gateway service 的架构演进");

        assert_eq!(decision.mode, ExecutionMode::PlanExecute);
        assert!(decision.confidence >= 70);
    }

    #[test]
    fn infers_write_dependency_on_prior_same_path_read() {
        let mut requests = vec![
            request("read-1", "read_file", r#"{"path":"src/lib.rs"}"#),
            request(
                "write-1",
                "write_file",
                r#"{"path":"src/lib.rs","content":"x"}"#,
            ),
        ];

        assert_eq!(infer_tool_dependencies(&mut requests), 1);
        assert_eq!(requests[1].depends_on, vec!["read-1"]);
    }

    #[test]
    fn infers_transaction_dependency_on_patch_plan() {
        let mut requests = vec![
            request(
                "plan-1",
                "patch_plan",
                r#"{"edits":[{"path":"src/lib.rs","old_string":"a","new_string":"b"}]}"#,
            ),
            request(
                "apply-1",
                "apply_patch_transaction",
                r#"{"edits":[{"path":"src/lib.rs","old_string":"a","new_string":"b"}]}"#,
            ),
        ];

        assert_eq!(infer_tool_dependencies(&mut requests), 1);
        assert_eq!(requests[1].depends_on, vec!["plan-1"]);
    }
}
