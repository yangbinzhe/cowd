//! First-turn context fanout planning.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::intent_planner::{TaskIntent, classify_intent};

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
                "grep_search",
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
                "grep_search",
                json!({"searches": [
                    {"pattern": "error|failed|panic|exception", "path": "."},
                    {"pattern": "#\\[test\\]|describe\\(|it\\(", "path": "."}
                ]}),
            ),
        ],
        TaskIntent::Test => Vec::new(),
        TaskIntent::Release => vec![
            call(
                "workspace_snapshot",
                json!({"include_git": true, "include_files": false}),
            ),
            call(
                "glob_search",
                json!({"patterns": [
                    {"pattern": "scripts/validate.sh"},
                    {"pattern": "**/Cargo.toml"},
                    {"pattern": "**/package.json"}
                ]}),
            ),
        ],
        TaskIntent::Frontend => vec![
            call(
                "workspace_snapshot",
                json!({"include_git": true, "include_files": true, "roots": ["crates", "webui"], "max_files": 500}),
            ),
            call(
                "glob_search",
                json!({"patterns": [
                    {"pattern": "**/*.tsx"},
                    {"pattern": "**/*.ts"},
                    {"pattern": "**/*.css"},
                    {"pattern": "**/*.rs"}
                ]}),
            ),
        ],
        TaskIntent::Docs => vec![call(
            "glob_search",
            json!({"patterns": [
                {"pattern": "docs/**/*.md"},
                {"pattern": "*.md"}
            ]}),
        )],
        TaskIntent::Backend | TaskIntent::Explore => vec![
            call(
                "workspace_snapshot",
                json!({"include_git": true, "include_files": true, "max_files": 500}),
            ),
            call(
                "grep_search",
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
        assert!(plan.calls.iter().any(|call| call.name == "glob_search"));
    }

    #[test]
    fn every_fanout_example_validates_against_real_tool_schemas() {
        let workspace = tempfile::tempdir().expect("workspace");
        let lease = tools::ToolHost::builtin("fanout-schema", workspace.path()).pin_snapshot();
        for intent in [
            TaskIntent::Review,
            TaskIntent::Bugfix,
            TaskIntent::Release,
            TaskIntent::Test,
            TaskIntent::Frontend,
            TaskIntent::Docs,
            TaskIntent::Backend,
            TaskIntent::Explore,
        ] {
            let plan = plan_context_fanout_for_intent(intent, "schema conformance");
            for call in plan.calls {
                lease
                    .validate_input(&call.name, &call.input)
                    .unwrap_or_else(|error| panic!("{intent:?} {}: {error}", call.name));
            }
        }
        for invalid in [
            json!({"calls": [{"name": "glob_search", "input": {"pattern": "*.rs"}}]}),
            json!({"patterns": [{"pattern": "*.rs"}]}),
        ] {
            assert!(lease.validate_input("tool_orchestrate", &invalid).is_err());
        }
    }

    #[test]
    fn verification_intent_does_not_prescribe_repository_inspection() {
        let plan = plan_context_fanout_for_intent(TaskIntent::Test, "verify file bytes");
        assert!(plan.calls.is_empty());
        for prompt in [
            "test result.txt bytes and final newline",
            "读取 input.txt 统计非空行，写入 result.txt 并验证实际字节",
        ] {
            let plan = plan_context_fanout(prompt);
            assert_eq!(plan.intent, TaskIntent::Test);
            assert!(plan.calls.is_empty());
            let evidence = crate::evidence_planner::plan_evidence(prompt);
            assert!(evidence.recommended_calls.is_empty());
        }
    }

    #[test]
    fn docs_fanout_stays_inside_workspace() {
        let plan = plan_context_fanout_for_intent(TaskIntent::Docs, "documentation");
        assert!(plan.calls.iter().all(|call| call.name == "glob_search"));
        for call in plan.calls {
            for pattern in call.input["patterns"].as_array().expect("patterns") {
                assert!(
                    !pattern["pattern"]
                        .as_str()
                        .expect("pattern")
                        .starts_with("../")
                );
            }
        }
    }
}
