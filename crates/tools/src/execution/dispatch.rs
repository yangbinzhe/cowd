//! Canonical builtin tool dispatch table.

use super::*;

pub(crate) fn execute_with_lease(
    lease: &ToolHostLease,
    name: &str,
    input: &Value,
) -> Result<String, String> {
    match name {
        "bash" => {
            // `shell` is a backend selector, not part of the bash/powershell
            // input contract (both deny unknown fields). Strip it for every
            // value, then route: powershell -> the PowerShell backend, anything
            // else (auto/bash/absent) -> the bash backend.
            let shell = input.get("shell").and_then(serde_json::Value::as_str);
            let mut restored = input.clone();
            if shell.is_some() {
                if let Some(object) = restored.as_object_mut() {
                    object.remove("shell");
                }
            }
            if shell == Some("powershell") {
                let ps_input: PowerShellInput = from_value(&restored)?;
                run_powershell(lease, ps_input)
            } else {
                let bash_input: BashCommandInput = from_value(&restored)?;
                run_bash(lease, bash_input)
            }
        }
        "background" => match input.get("action").and_then(|value| value.as_str()) {
            Some("status") => {
                // `action` selects the operation and is not part of the
                // per-action contract, which denies unknown fields.
                let mut stripped = input.clone();
                if let Some(object) = stripped.as_object_mut() {
                    object.remove("action");
                }
                run_background_task_status(lease, &stripped)
            }
            Some("kill") => {
                let mut stripped = input.clone();
                if let Some(object) = stripped.as_object_mut() {
                    object.remove("action");
                }
                run_background_task_kill(lease, &stripped)
            }
            other => Err(format!(
                "background action must be `status` or `kill`, got {other:?}"
            )),
        },
        "read_file" => {
            from_value::<ReadFileInput>(input).and_then(|parsed| run_read_file(lease, parsed))
        }
        "edit" => {
            let action = input.get("action").and_then(serde_json::Value::as_str);
            let mut rest = input.clone();
            if let Some(object) = rest.as_object_mut() {
                object.remove("action");
            }
            match action {
                Some("write") => from_value::<WriteFileInput>(&rest)
                    .and_then(|parsed| run_write_file(lease, parsed)),
                Some("replace") => from_value::<EditFileInput>(&rest)
                    .and_then(|parsed| run_edit_file(lease, parsed)),
                Some("preview") => from_value::<MutationPreviewInput>(&rest)
                    .and_then(|parsed| run_mutation_preview(lease, parsed)),
                Some("patch") => from_value::<MutationApplyInput>(&rest)
                    .and_then(|parsed| run_apply_patch_transaction(lease, parsed)),
                Some("notebook") => from_value::<NotebookEditInput>(&rest)
                    .and_then(|parsed| run_notebook_edit(lease, parsed)),
                other => Err(format!(
                    "edit action must be write|replace|preview|patch|notebook, got {other:?}"
                )),
            }
        }
        "checkpoint" => match input.get("action").and_then(|value| value.as_str()) {
            Some("create") => from_value::<CheckpointCreateInput>(input)
                .and_then(|parsed| run_checkpoint_create(lease, parsed)),
            Some("list") => run_checkpoint_list(lease),
            Some("diff") => from_value::<CheckpointDiffInput>(input)
                .and_then(|parsed| run_checkpoint_diff(lease, parsed)),
            Some("restore") => from_value::<CheckpointRestoreInput>(input)
                .and_then(|parsed| run_checkpoint_restore(lease, parsed)),
            other => Err(format!(
                "checkpoint action must be create|list|diff|restore, got {other:?}"
            )),
        },
        "glob_search" => {
            if input.get("patterns").is_some() {
                from_value::<GlobSearchManyInput>(input)
                    .and_then(|parsed| run_glob_search_many(lease, parsed))
            } else {
                from_value::<GlobSearchInputValue>(input)
                    .and_then(|parsed| run_glob_search(lease, parsed))
            }
        }
        "grep_search" => {
            if input.get("searches").is_some() {
                from_value::<GrepSearchBatchInput>(input).and_then(|parsed| run_grep_search_batch(lease, parsed))
            } else if input.get("language").is_some() {
                from_value::<GrepSearchLanguageInput>(input)
                    .and_then(|parsed| run_grep_search_language(lease, parsed))
            } else {
                from_value::<GrepSearchInput>(input)
                    .and_then(|parsed| run_grep_search(lease, parsed))
            }
        }
        "workspace_snapshot" => from_value::<WorkspaceSnapshotInput>(input)
            .and_then(|parsed| run_workspace_snapshot(lease, parsed)),
        "tool_cache_stats" => to_pretty_json(lease.cache().stats()),
        "tool_orchestrate" => run_orchestrate(lease, input),
        "web_fetch" => from_value::<WebFetchInput>(input).and_then(run_web_fetch),
        "web_search" => from_value::<WebSearchInput>(input).and_then(run_web_search),
        "skill" => {
            // The merged family's own `action` field is not part of the per-action
            // input contracts, which deny unknown fields.
            let action = input.get("action").and_then(serde_json::Value::as_str);
            let mut rest = input.clone();
            if let Some(object) = rest.as_object_mut() {
                object.remove("action");
            }
            match action {
                Some("install_plan") => from_value::<SkillInstallPlanInput>(&rest)
                    .and_then(|parsed| run_skill_install_plan(lease, parsed)),
                Some("install_commit") => from_value::<SkillInstallCommitInput>(&rest)
                    .and_then(|parsed| run_skill_install_commit(lease, parsed)),
                Some("status") => from_value::<SkillStatusInput>(&rest).and_then(run_skill_status),
                Some("rollback") => from_value::<SkillRollbackInput>(&rest)
                    .and_then(|parsed| run_skill_rollback(lease, parsed)),
                Some("deactivate") => from_value::<SkillStatusInput>(&rest)
                    .and_then(|parsed| run_skill_deactivate(lease, parsed)),
                other => Err(format!(
                    "skill action must be install_plan|install_commit|status|rollback|deactivate, got {other:?}"
                )),
            }
        }
        "todo_write" => {
            from_value::<TodoWriteInput>(input).and_then(|parsed| run_todo_write(lease, parsed))
        }
        "tool_search" => {
            from_value::<ToolSearchInput>(input).and_then(|parsed| run_tool_search(lease, parsed))
        }
        "current_time" => run_current_time(),
        "get_context_remaining" => run_get_context_remaining(input),
        "request_plugin_install" => {
            let plugin_id = input
                .get("plugin_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            Err(format!(
                "request_plugin_install is not supported: plugin `{plugin_id}` cannot be installed by a model. \
                 Plugin installation is an operator control-plane operation performed through the Gateway."
            ))
        }
        "sleep" => from_value::<SleepInput>(input).and_then(run_sleep),
        "config" => from_value::<ConfigInput>(input).and_then(|parsed| run_config(lease, parsed)),
        "plan_mode" => match input.get("action").and_then(|value| value.as_str()) {
            Some("enter") => {
                from_value::<EnterPlanModeInput>(input).and_then(|parsed| run_enter_plan_mode(lease, parsed))
            }
            Some("exit") => {
                from_value::<ExitPlanModeInput>(input).and_then(|parsed| run_exit_plan_mode(lease, parsed))
            }
            other => Err(format!(
                "plan_mode action must be `enter` or `exit`, got {other:?}"
            )),
        },
        "structured_output" => {
            from_value::<StructuredOutputInput>(input).and_then(run_structured_output)
        }
        "ask" => {
            if input.get("message").is_some() && input.get("question").is_some() {
                return Err(
                    "ask is either a `question` (blocking) or a `message` (brief), not both"
                        .to_string(),
                );
            }
            if input.get("message").is_some() {
                from_value::<BriefInput>(input).and_then(|parsed| run_brief(lease, parsed))
            } else {
                from_value::<AskUserQuestionInput>(input).and_then(run_ask_user_question)
            }
        }
        "lsp" => from_value::<LspInput>(input).and_then(|parsed| run_lsp(lease, parsed)),
        "remote_trigger" => from_value::<RemoteTriggerInput>(input).and_then(run_remote_trigger),
        "mcp" => match input.get("action").and_then(|value| value.as_str()) {
            Some("call") => {
                from_value::<McpToolInput>(input).and_then(|parsed| run_mcp_tool(lease, parsed))
            }
            Some("list") => from_value::<McpResourceInput>(input)
                .and_then(|parsed| run_list_mcp_resources(lease, parsed)),
            Some("read") => from_value::<McpResourceInput>(input)
                .and_then(|parsed| run_read_mcp_resource(lease, parsed)),
            Some("auth") => {
                from_value::<McpAuthInput>(input).and_then(|parsed| run_mcp_auth(lease, parsed))
            }
            other => Err(format!(
                "mcp action must be call|list|read|auth, got {other:?}"
            )),
        },
        "vision_analyze" => run_vision_analyze(lease, input),
        "execute_code" => {
            use crate::sandbox_exec::execute_code_in_workspace;
            let lang = input
                .get("language")
                .and_then(|v| v.as_str())
                .unwrap_or("python");
            let code = input.get("code").and_then(|v| v.as_str()).unwrap_or("");
            let timeout_ms = input.get("timeout_ms").and_then(serde_json::Value::as_u64);
            let result = execute_code_in_workspace(lang, code, timeout_ms, lease.workspace_root());
            Ok(serde_json::to_string_pretty(&serde_json::json!({
                "stdout": result.stdout,
                "stderr": result.stderr,
                "exit_code": result.exit_code,
            }))
            .unwrap_or_default())
        }
        _ => Err(format!("unsupported tool: {name}")),
    }
}
