//! Lightweight intent and dependency planning for tool execution.

use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    let normalized = prompt.to_ascii_lowercase();
    let (intent, recommended_tools, reason) =
        if contains_any(&normalized, &["review", "审查", "审计"]) {
            (
                TaskIntent::Review,
                vec!["workspace_snapshot", "grep_many", "read_many"],
                "review tasks need workspace status, changed files, and targeted reads",
            )
        } else if contains_any(&normalized, &["bug", "fix", "修复", "报错", "失败"]) {
            (
                TaskIntent::Bugfix,
                vec!["grep_many", "read_many", "tool_batch_readonly"],
                "bugfix tasks need symbol search, callers, and failing test context",
            )
        } else if contains_any(
            &normalized,
            &["frontend", "ui", "页面", "样式", "tui", "webui"],
        ) {
            (
                TaskIntent::Frontend,
                vec!["workspace_snapshot", "glob_many", "read_many"],
                "frontend tasks need component, style, and route context",
            )
        } else if contains_any(&normalized, &["release", "发布", "tag", "验收"]) {
            (
                TaskIntent::Release,
                vec!["workspace_snapshot", "tool_batch_readonly"],
                "release tasks need status, tests, build, and release-gate context",
            )
        } else if contains_any(&normalized, &["test", "测试", "e2e", "验证"]) {
            (
                TaskIntent::Test,
                vec!["workspace_snapshot", "grep_many"],
                "test tasks need test targets and validation commands",
            )
        } else if contains_any(&normalized, &["docs", "文档", "方案"]) {
            (
                TaskIntent::Docs,
                vec!["glob_many", "read_many"],
                "docs tasks need related document discovery and batch reads",
            )
        } else if contains_any(&normalized, &["backend", "runtime", "server", "后端"]) {
            (
                TaskIntent::Backend,
                vec!["workspace_snapshot", "grep_many", "read_many"],
                "backend tasks need runtime/module search and source context",
            )
        } else {
            (
                TaskIntent::Explore,
                vec!["workspace_snapshot", "grep_many", "read_many"],
                "exploration starts with workspace snapshot and fanout search/read",
            )
        };

    IntentPlan {
        intent,
        recommended_tools: recommended_tools.into_iter().map(str::to_string).collect(),
        reason: reason.to_string(),
    }
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

fn contains_any(value: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| value.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;

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
