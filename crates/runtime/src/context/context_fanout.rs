//! First-turn context fanout planning.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::intent_planner::{classify_intent, TaskIntent};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FanoutToolCall {
    pub name: String,
    pub input: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextFanoutPlan {
    pub intent: TaskIntent,
    pub calls: Vec<FanoutToolCall>,
    pub reason: String,
}

#[must_use]
pub fn plan_context_fanout(prompt: &str) -> ContextFanoutPlan {
    let intent = classify_intent(prompt);
    plan_context_fanout_for_intent(intent.intent, intent.reason)
}

#[must_use]
pub fn plan_context_fanout_for_intent(
    intent: TaskIntent,
    reason: impl Into<String>,
) -> ContextFanoutPlan {
    let calls = match intent {
        TaskIntent::Review => vec![
            call(
                "workspace_snapshot",
                json!({"include_git": true, "include_files": true, "max_files": 400}),
            ),
            call(
                "grep_many",
                json!({"searches": [
                    {"pattern": "TODO|FIXME|panic!|unwrap\\(", "path": ".", "glob": "*.rs"},
                    {"pattern": "cargo test|validate.sh", "path": "."}
                ]}),
            ),
        ],
        TaskIntent::Bugfix => vec![
            call(
                "workspace_snapshot",
                json!({"include_git": true, "include_files": true, "max_files": 300}),
            ),
            call(
                "grep_many",
                json!({"searches": [
                    {"pattern": "error|failed|panic|exception", "path": "."},
                    {"pattern": "#\\[test\\]|describe\\(|it\\(", "path": "."}
                ]}),
            ),
        ],
        TaskIntent::Release | TaskIntent::Test => vec![
            call(
                "workspace_snapshot",
                json!({"include_git": true, "include_files": false}),
            ),
            call(
                "tool_batch_readonly",
                json!({"calls": [
                    {"name": "glob_search", "input": {"pattern": "scripts/validate.sh"}},
                    {"name": "glob_search", "input": {"pattern": "**/Cargo.toml"}},
                    {"name": "glob_search", "input": {"pattern": "**/package.json"}}
                ]}),
            ),
        ],
        TaskIntent::Frontend => vec![
            call(
                "workspace_snapshot",
                json!({"include_git": true, "include_files": true, "roots": ["crates", "webui"], "max_files": 500}),
            ),
            call(
                "glob_many",
                json!({"patterns": [
                    {"pattern": "**/*.tsx"},
                    {"pattern": "**/*.ts"},
                    {"pattern": "**/*.css"},
                    {"pattern": "**/*.rs"}
                ]}),
            ),
        ],
        TaskIntent::Docs => vec![call(
            "glob_many",
            json!({"patterns": [
                {"pattern": "docs/**/*.md"},
                {"pattern": "../plan/**/*.md"},
                {"pattern": "*.md"}
            ]}),
        )],
        TaskIntent::Backend | TaskIntent::Explore => vec![
            call(
                "workspace_snapshot",
                json!({"include_git": true, "include_files": true, "max_files": 500}),
            ),
            call(
                "grep_many",
                json!({"searches": [
                    {"pattern": "pub struct|pub enum|pub fn", "path": "crates", "glob": "*.rs", "head_limit": 40},
                    {"pattern": "ToolExecutor|execute_tool|RuntimeEvent", "path": "crates", "glob": "*.rs", "head_limit": 80}
                ]}),
            ),
        ],
    };

    ContextFanoutPlan {
        intent,
        calls,
        reason: reason.into(),
    }
}

fn call(name: &str, input: Value) -> FanoutToolCall {
    FanoutToolCall {
        name: name.to_string(),
        input,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn review_plan_starts_with_workspace_snapshot() {
        let plan = plan_context_fanout("review this branch");
        assert_eq!(plan.intent, TaskIntent::Review);
        assert_eq!(plan.calls[0].name, "workspace_snapshot");
    }

    #[test]
    fn release_plan_uses_batch_readonly() {
        let plan = plan_context_fanout("发布前验收");
        assert_eq!(plan.intent, TaskIntent::Release);
        assert!(plan
            .calls
            .iter()
            .any(|call| call.name == "tool_batch_readonly"));
    }
}
