use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ToolExecutionContext {
    pub cwd: PathBuf,
    pub turn_id: String,
}

impl ToolExecutionContext {
    pub(crate) fn from_current_dir(turn_id: impl Into<String>) -> Result<Self, String> {
        Ok(Self {
            cwd: std::env::current_dir().map_err(|error| error.to_string())?,
            turn_id: turn_id.into(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum PreparedReadonlyLeaf {
    ReadFile(Value),
    GlobSearch(Value),
    GrepSearch(Value),
    WorkspaceSnapshot(Value),
    ToolCacheStats,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct PreparedToolInvocation {
    pub call_id: String,
    pub original_name: String,
    pub normalized_name: String,
    pub resource_scope: String,
    pub output_budget: usize,
    pub leaf: PreparedReadonlyLeaf,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct PreparedToolCall {
    pub name: String,
    pub input: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PreparedToolError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

impl PreparedToolError {
    pub(crate) fn validation(message: impl Into<String>) -> Self {
        Self {
            code: "validation_error".to_string(),
            message: message.into(),
            retryable: false,
        }
    }
}

pub(crate) fn prepare_readonly_invocations(
    context: &ToolExecutionContext,
    calls: &[PreparedToolCall],
) -> Result<Vec<PreparedToolInvocation>, PreparedToolError> {
    calls
        .iter()
        .enumerate()
        .map(|(index, call)| prepare_readonly_invocation(context, index, call))
        .collect()
}

fn prepare_readonly_invocation(
    context: &ToolExecutionContext,
    index: usize,
    call: &PreparedToolCall,
) -> Result<PreparedToolInvocation, PreparedToolError> {
    let normalized_name = normalize_tool_name(&call.name);
    let leaf = match normalized_name.as_str() {
        "read_file" => PreparedReadonlyLeaf::ReadFile(call.input.clone()),
        "glob_search" => PreparedReadonlyLeaf::GlobSearch(call.input.clone()),
        "grep_search" => PreparedReadonlyLeaf::GrepSearch(call.input.clone()),
        "workspace_snapshot" => PreparedReadonlyLeaf::WorkspaceSnapshot(call.input.clone()),
        "tool_cache_stats" => PreparedReadonlyLeaf::ToolCacheStats,
        _ => {
            return Err(PreparedToolError::validation(format!(
                "tool `{}` is not supported by prepared readonly execution",
                call.name
            )));
        }
    };

    Ok(PreparedToolInvocation {
        call_id: format!("{}:prepared:{index}", context.turn_id),
        original_name: call.name.clone(),
        normalized_name: normalized_name.clone(),
        resource_scope: infer_resource_scope(&normalized_name, &call.input),
        output_budget: 256 * 1024,
        leaf,
    })
}

pub(crate) fn normalize_tool_name(name: &str) -> String {
    name.trim().replace('-', "_").to_ascii_lowercase()
}

fn infer_resource_scope(tool_name: &str, input: &Value) -> String {
    match tool_name {
        "read_file" => input
            .get("path")
            .and_then(Value::as_str)
            .map(|path| format!("file:{path}"))
            .unwrap_or_else(|| "file:unknown".to_string()),
        "glob_search" | "grep_search" => input
            .get("path")
            .and_then(Value::as_str)
            .map(|path| format!("directory:{path}"))
            .unwrap_or_else(|| "workspace:.".to_string()),
        "workspace_snapshot" => "workspace:.".to_string(),
        "tool_cache_stats" => "runtime:tool_cache".to_string(),
        _ => "unknown".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn prepares_supported_readonly_leaf_tools() {
        let context = ToolExecutionContext {
            cwd: PathBuf::from("/tmp/workspace"),
            turn_id: "turn-1".to_string(),
        };
        let prepared = prepare_readonly_invocations(
            &context,
            &[
                PreparedToolCall {
                    name: "read_file".to_string(),
                    input: json!({"path": "src/lib.rs"}),
                },
                PreparedToolCall {
                    name: "tool_cache_stats".to_string(),
                    input: json!({}),
                },
            ],
        )
        .expect("prepare");

        assert_eq!(prepared.len(), 2);
        assert_eq!(prepared[0].normalized_name, "read_file");
        assert_eq!(prepared[0].resource_scope, "file:src/lib.rs");
        assert!(matches!(
            prepared[1].leaf,
            PreparedReadonlyLeaf::ToolCacheStats
        ));
    }

    #[test]
    fn rejects_unsupported_prepared_tool() {
        let context = ToolExecutionContext {
            cwd: PathBuf::from("/tmp/workspace"),
            turn_id: "turn-1".to_string(),
        };
        let err = prepare_readonly_invocations(
            &context,
            &[PreparedToolCall {
                name: "write_file".to_string(),
                input: json!({"path": "x", "content": "no"}),
            }],
        )
        .expect_err("write_file must be rejected");

        assert_eq!(err.code, "validation_error");
        assert!(err.message.contains("write_file"));
    }
}
