//! Canonical builtin tool dispatch table.

use super::*;

pub(crate) fn execute_with_lease(
    lease: &ToolHostLease,
    name: &str,
    input: &Value,
) -> Result<String, String> {
    match name {
        "bash" => {
            let bash_input: BashCommandInput = from_value(input)?;
            run_bash(lease, bash_input)
        }
        "read_file" => {
            from_value::<ReadFileInput>(input).and_then(|parsed| run_read_file(lease, parsed))
        }
        "read_many" => {
            from_value::<ReadManyInput>(input).and_then(|parsed| run_read_many(lease, parsed))
        }
        "write_file" => {
            from_value::<WriteFileInput>(input).and_then(|parsed| run_write_file(lease, parsed))
        }
        "edit_file" => {
            from_value::<EditFileInput>(input).and_then(|parsed| run_edit_file(lease, parsed))
        }
        "mutation_preview" | "edit_many_preview" | "patch_plan" => {
            from_value::<MutationPreviewInput>(input)
                .and_then(|parsed| run_mutation_preview(lease, parsed))
        }
        "apply_patch_transaction" => from_value::<MutationApplyInput>(input)
            .and_then(|parsed| run_apply_patch_transaction(lease, parsed)),
        "checkpoint_create" => from_value::<CheckpointCreateInput>(input)
            .and_then(|parsed| run_checkpoint_create(lease, parsed)),
        "checkpoint_list" => run_checkpoint_list(lease),
        "checkpoint_diff" => from_value::<CheckpointDiffInput>(input)
            .and_then(|parsed| run_checkpoint_diff(lease, parsed)),
        "checkpoint_restore" => from_value::<CheckpointRestoreInput>(input)
            .and_then(|parsed| run_checkpoint_restore(lease, parsed)),
        "glob_search" => from_value::<GlobSearchInputValue>(input)
            .and_then(|parsed| run_glob_search(lease, parsed)),
        "glob_many" => {
            from_value::<GlobManyInput>(input).and_then(|parsed| run_glob_many(lease, parsed))
        }
        "grep_search" => {
            from_value::<GrepSearchInput>(input).and_then(|parsed| run_grep_search(lease, parsed))
        }
        "grep_many" => {
            from_value::<GrepManyInput>(input).and_then(|parsed| run_grep_many(lease, parsed))
        }
        "ast_grep_search" => from_value::<AstGrepSearchInput>(input)
            .and_then(|parsed| run_ast_grep_search(lease, parsed)),
        "workspace_snapshot" => from_value::<WorkspaceSnapshotInput>(input)
            .and_then(|parsed| run_workspace_snapshot(lease, parsed)),
        "tool_batch_readonly" => from_value::<ToolBatchReadonlyInput>(input)
            .and_then(|parsed| run_tool_batch_readonly(lease, parsed)),
        "tool_cache_stats" => to_pretty_json(lease.cache().stats()),
        "web_fetch" => from_value::<WebFetchInput>(input).and_then(run_web_fetch),
        "web_search" => from_value::<WebSearchInput>(input).and_then(run_web_search),
        "skill_install_plan" => from_value::<SkillInstallPlanInput>(input)
            .and_then(|parsed| run_skill_install_plan(lease, parsed)),
        "skill_install_commit" => from_value::<SkillInstallCommitInput>(input)
            .and_then(|parsed| run_skill_install_commit(lease, parsed)),
        "skill_status" => from_value::<SkillStatusInput>(input).and_then(run_skill_status),
        "skill_rollback" => from_value::<SkillRollbackInput>(input)
            .and_then(|parsed| run_skill_rollback(lease, parsed)),
        "skill_deactivate" => from_value::<SkillStatusInput>(input)
            .and_then(|parsed| run_skill_deactivate(lease, parsed)),
        "todo_write" => {
            from_value::<TodoWriteInput>(input).and_then(|parsed| run_todo_write(lease, parsed))
        }
        "question" => {
            let q = input.get("question").and_then(|v| v.as_str()).unwrap_or("");
            let opts = input.get("options").and_then(|v| v.as_array()).map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            });
            Ok(format!(
                "[QUESTION] {q}{}",
                opts.map(|o| format!("\nOptions: {o}")).unwrap_or_default()
            ))
        }
        "ast_search" => {
            let parsed = serde_json::from_value::<AstGrepSearchInput>(input.clone()).unwrap_or(
                AstGrepSearchInput {
                    pattern: input
                        .get("pattern")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    language: input
                        .get("language")
                        .and_then(|value| value.as_str())
                        .unwrap_or("rust")
                        .to_string(),
                    path: None,
                    case_sensitive: false,
                    max_files: 200,
                    max_matches: 50,
                },
            );
            run_ast_grep_search(lease, parsed)
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
        "notebook_edit" => from_value::<NotebookEditInput>(input)
            .and_then(|parsed| run_notebook_edit(lease, parsed)),
        "sleep" => from_value::<SleepInput>(input).and_then(run_sleep),
        "send_user_message" => {
            from_value::<BriefInput>(input).and_then(|parsed| run_brief(lease, parsed))
        }
        "config" => from_value::<ConfigInput>(input).and_then(|parsed| run_config(lease, parsed)),
        "enter_plan_mode" => from_value::<EnterPlanModeInput>(input)
            .and_then(|parsed| run_enter_plan_mode(lease, parsed)),
        "exit_plan_mode" => from_value::<ExitPlanModeInput>(input)
            .and_then(|parsed| run_exit_plan_mode(lease, parsed)),
        "structured_output" => {
            from_value::<StructuredOutputInput>(input).and_then(run_structured_output)
        }
        "repl" => from_value::<ReplInput>(input).and_then(|parsed| run_repl(lease, parsed)),
        "power_shell" => {
            let ps_input: PowerShellInput = from_value(input)?;
            run_powershell(lease, ps_input)
        }
        "ask_user_question" => {
            from_value::<AskUserQuestionInput>(input).and_then(run_ask_user_question)
        }
        "lsp" => from_value::<LspInput>(input).and_then(|parsed| run_lsp(lease, parsed)),
        "list_mcp_resources" => from_value::<McpResourceInput>(input)
            .and_then(|parsed| run_list_mcp_resources(lease, parsed)),
        "read_mcp_resource" => from_value::<McpResourceInput>(input)
            .and_then(|parsed| run_read_mcp_resource(lease, parsed)),
        "mcp_auth" => {
            from_value::<McpAuthInput>(input).and_then(|parsed| run_mcp_auth(lease, parsed))
        }
        "remote_trigger" => from_value::<RemoteTriggerInput>(input).and_then(run_remote_trigger),
        "mcp" => from_value::<McpToolInput>(input).and_then(|parsed| run_mcp_tool(lease, parsed)),
        "testing_permission" => {
            from_value::<TestingPermissionInput>(input).and_then(run_testing_permission)
        }
        "vision_analyze" => run_vision_analyze(lease, input),
        "execute_code" => {
            use crate::sandbox_exec::execute_code_in_workspace;
            let lang = input
                .get("language")
                .and_then(|v| v.as_str())
                .unwrap_or("python");
            let code = input.get("code").and_then(|v| v.as_str()).unwrap_or("");
            let result = execute_code_in_workspace(lang, code, None, lease.workspace_root());
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
