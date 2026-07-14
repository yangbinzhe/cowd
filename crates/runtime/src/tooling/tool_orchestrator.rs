//! Intrinsic execution characteristics for runtime tool planning.
//!
//! This module intentionally contains no registry, cache, execution state, or
//! result projection. Tool inventory and execution belong to `tools::ToolHost`;
//! runtime only uses these deterministic profiles while planning work.

use serde::{Deserialize, Serialize};

use crate::bash_validation::{classify_command, CommandIntent};

// ---------------------------------------------------------------------------
// ToolSafetyCategory
// ---------------------------------------------------------------------------

/// Safety classification for tool execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolSafetyCategory {
    /// Read-only tools — safe to run concurrently.
    ReadOnly,
    /// Local file writes — serialized per file path, different files can run concurrently.
    WriteLocal,
    /// Network access tools — limited concurrency (default: 3).
    Network,
    /// Destructive operations — require explicit confirmation.
    Destructive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCachePolicy {
    ScopedRead,
    GlobalRead,
    ScopeInvalidatingWrite,
    GlobalInvalidatingWrite,
    Uncached,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolExecutionProfile {
    pub name: String,
    pub safety_category: ToolSafetyCategory,
    pub max_concurrency: usize,
    pub timeout_secs: u64,
    pub cache_policy: ToolCachePolicy,
    pub prepared_readonly_supported: bool,
}

impl ToolSafetyCategory {
    /// Classify a tool by name using a built-in mapping.
    pub fn from_tool_name(name: &str) -> Self {
        let normalized = normalize_tool_name_for_safety(name);
        match normalized.as_str() {
            "read"
            | "read_file"
            | "read_many"
            | "cat"
            | "head"
            | "tail"
            | "grep"
            | "grep_search"
            | "grep_many"
            | "rg"
            | "glob"
            | "glob_search"
            | "glob_many"
            | "find"
            | "ls"
            | "list_directory"
            | "file_search"
            | "workspace_snapshot"
            | "tool_batch_readonly"
            | "tool_cache_stats"
            | "mutation_preview"
            | "edit_many_preview"
            | "patch_plan"
            | "checkpoint_list"
            | "checkpoint_diff"
            | "git_status"
            | "git_log"
            | "git_diff"
            | "git_show"
            | "memory_search"
            | "memory_list"
            | "memory_get"
            | "session_list"
            | "session_get"
            | "skill_list"
            | "skill_view"
            | "question"
            | "ask_user_question"
            | "tool_search"
            | "runtime_capabilities"
            | "list_mcp_resources"
            | "read_mcp_resource" => Self::ReadOnly,

            "add" | "calculator" => Self::ReadOnly,

            "write"
            | "write_file"
            | "edit"
            | "edit_file"
            | "create_file"
            | "delete_file"
            | "memory_create"
            | "memory_delete"
            | "session_create"
            | "todo_write"
            | "apply_patch_transaction"
            | "checkpoint_create"
            | "checkpoint_restore" => Self::WriteLocal,

            "web_search" | "web_fetch" | "http_request" => Self::Network,

            "bash"
            | "powershell"
            | "repl"
            | "mcp"
            | "mcp_auth"
            | "remote_trigger"
            | "agent"
            | "runtime_orchestrate"
            | "task_create"
            | "run_task_packet"
            | "task_stop"
            | "task_update"
            | "worker_create"
            | "worker_send_prompt"
            | "worker_restart"
            | "worker_terminate"
            | "team_create"
            | "team_delete"
            | "cron_create"
            | "cron_delete"
            | "config"
            | "notebook_edit"
            | "structured_output"
            | "execute_code"
            | "rm"
            | "kill"
            | "sudo"
            | "truncate"
            | "drop" => Self::Destructive,

            _ if normalized.starts_with("read")
                || normalized.starts_with("grep")
                || normalized.starts_with("glob")
                || normalized.starts_with("lsp") =>
            {
                Self::ReadOnly
            }
            _ if normalized.starts_with("write")
                || normalized.starts_with("edit")
                || normalized.starts_with("ast_grep") =>
            {
                Self::WriteLocal
            }
            // Unknown tools are treated as local mutations until ToolHost
            // supplies an authoritative effect descriptor.
            _ => Self::WriteLocal,
        }
    }

    /// Maximum concurrent executions for this category.
    pub fn max_concurrency(&self) -> usize {
        match self {
            Self::ReadOnly => usize::MAX,
            Self::WriteLocal => 4,
            Self::Network => 3,
            Self::Destructive => 1,
        }
    }

    /// Default timeout in seconds for tools in this category.
    /// ReadOnly tools get 30s (fast read operations), others get 120s.
    pub fn default_timeout_secs(&self) -> u64 {
        match self {
            Self::ReadOnly => 30,
            Self::WriteLocal | Self::Network | Self::Destructive => 120,
        }
    }
}

#[must_use]
pub fn tool_execution_profile(tool_name: &str) -> ToolExecutionProfile {
    let normalized = normalize_tool_name_for_safety(tool_name);
    let safety_category = ToolSafetyCategory::from_tool_name(&normalized);
    ToolExecutionProfile {
        name: normalized.clone(),
        safety_category,
        max_concurrency: safety_category.max_concurrency(),
        timeout_secs: safety_category.default_timeout_secs(),
        cache_policy: cache_policy_for_tool(&normalized, safety_category),
        prepared_readonly_supported: prepared_readonly_supported(&normalized),
    }
}

fn cache_policy_for_tool(
    normalized_name: &str,
    safety_category: ToolSafetyCategory,
) -> ToolCachePolicy {
    match normalized_name {
        "read_file" | "glob_search" | "grep_search" | "workspace_snapshot" => {
            ToolCachePolicy::ScopedRead
        }
        "tool_cache_stats" => ToolCachePolicy::GlobalRead,
        "write_file" | "edit_file" | "apply_patch_transaction" => {
            ToolCachePolicy::ScopeInvalidatingWrite
        }
        "checkpoint_restore" => ToolCachePolicy::GlobalInvalidatingWrite,
        _ if safety_category == ToolSafetyCategory::ReadOnly => ToolCachePolicy::Uncached,
        _ => ToolCachePolicy::Uncached,
    }
}

fn prepared_readonly_supported(normalized_name: &str) -> bool {
    matches!(
        normalized_name,
        "read_file" | "glob_search" | "grep_search" | "workspace_snapshot" | "tool_cache_stats"
    )
}

fn normalize_tool_name_for_safety(name: &str) -> String {
    let normalized = name.trim().replace('-', "_").to_ascii_lowercase();
    match normalized.as_str() {
        "webfetch" => "web_fetch".to_string(),
        "websearch" => "web_search".to_string(),
        "todowrite" => "todo_write".to_string(),
        "askuserquestion" => "ask_user_question".to_string(),
        "toolsearch" => "tool_search".to_string(),
        "runtimecapabilities" => "runtime_capabilities".to_string(),
        "runtimeorchestrate" => "runtime_orchestrate".to_string(),
        "listmcpresources" => "list_mcp_resources".to_string(),
        "readmcpresource" => "read_mcp_resource".to_string(),
        "mcpauth" => "mcp_auth".to_string(),
        "remotetrigger" => "remote_trigger".to_string(),
        "notebookedit" => "notebook_edit".to_string(),
        "structuredoutput" => "structured_output".to_string(),
        "execute_code" => "execute_code".to_string(),
        "taskcreate" => "task_create".to_string(),
        "runtaskpacket" => "run_task_packet".to_string(),
        "taskget" => "task_get".to_string(),
        "tasklist" => "task_list".to_string(),
        "taskstop" => "task_stop".to_string(),
        "taskupdate" => "task_update".to_string(),
        "taskoutput" => "task_output".to_string(),
        "workercreate" => "worker_create".to_string(),
        "workerget" => "worker_get".to_string(),
        "workerobserve" => "worker_observe".to_string(),
        "workerresolvertrust" => "worker_resolve_trust".to_string(),
        "workerawaitready" => "worker_await_ready".to_string(),
        "workersendprompt" => "worker_send_prompt".to_string(),
        "workerrestart" => "worker_restart".to_string(),
        "workerterminate" => "worker_terminate".to_string(),
        "workerobservecompletion" => "worker_observe_completion".to_string(),
        "teamcreate" => "team_create".to_string(),
        "teamdelete" => "team_delete".to_string(),
        "croncreate" => "cron_create".to_string(),
        "crondelete" => "cron_delete".to_string(),
        "cronlist" => "cron_list".to_string(),
        other => other.to_string(),
    }
}

/// Classify a concrete invocation without retaining process-global policy.
#[must_use]
pub fn classify_tool_request(tool_name: &str, input: &str) -> ToolSafetyCategory {
    let normalized = normalize_tool_name_for_safety(tool_name);
    let fallback = ToolSafetyCategory::from_tool_name(&normalized);
    if normalized != "bash" {
        return fallback;
    }

    let command = serde_json::from_str::<serde_json::Value>(input)
        .ok()
        .and_then(|value| {
            value
                .get("command")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        });
    command.map_or(fallback, |command| match classify_command(&command) {
        CommandIntent::ReadOnly => ToolSafetyCategory::ReadOnly,
        CommandIntent::Write => ToolSafetyCategory::WriteLocal,
        CommandIntent::Network => ToolSafetyCategory::Network,
        CommandIntent::Destructive
        | CommandIntent::ProcessManagement
        | CommandIntent::PackageManagement
        | CommandIntent::SystemAdmin
        | CommandIntent::Unknown => ToolSafetyCategory::Destructive,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_intrinsic_tool_profiles() {
        assert_eq!(
            ToolSafetyCategory::from_tool_name("read_file"),
            ToolSafetyCategory::ReadOnly
        );
        assert_eq!(
            ToolSafetyCategory::from_tool_name("write_file"),
            ToolSafetyCategory::WriteLocal
        );
        assert_eq!(
            ToolSafetyCategory::from_tool_name("WebSearch"),
            ToolSafetyCategory::Network
        );
        assert_eq!(
            ToolSafetyCategory::from_tool_name("RuntimeOrchestrate"),
            ToolSafetyCategory::Destructive
        );
        assert_eq!(
            ToolSafetyCategory::from_tool_name("lsp_definition"),
            ToolSafetyCategory::ReadOnly
        );
    }

    #[test]
    fn shell_invocation_is_refined_without_a_registry() {
        assert_eq!(
            classify_tool_request("bash", r#"{"command":"git status"}"#),
            ToolSafetyCategory::ReadOnly
        );
        assert_eq!(
            classify_tool_request("bash", r#"{"command":"curl https://example.com"}"#),
            ToolSafetyCategory::Network
        );
        assert_eq!(
            classify_tool_request("bash", r#"{"command":"rm -rf target"}"#),
            ToolSafetyCategory::Destructive
        );
    }

    #[test]
    fn execution_profile_exposes_intrinsic_planning_hints() {
        let read = tool_execution_profile("read_file");
        assert_eq!(read.safety_category, ToolSafetyCategory::ReadOnly);
        assert_eq!(read.cache_policy, ToolCachePolicy::ScopedRead);
        assert!(read.prepared_readonly_supported);

        let write = tool_execution_profile("write_file");
        assert_eq!(write.safety_category, ToolSafetyCategory::WriteLocal);
        assert_eq!(write.cache_policy, ToolCachePolicy::ScopeInvalidatingWrite);
    }
}
