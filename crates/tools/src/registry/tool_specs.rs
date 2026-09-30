use serde_json::{json, Value};

use crate::permissions::PermissionMode;
use harness_contract::tool::ToolEffectResolverSpec;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolSpec {
    pub name: &'static str,
    pub description: &'static str,
    pub input_schema: Value,
    pub required_permission: PermissionMode,
}

/// Effect behavior is selected by the registration manifest, not inferred by
/// Runtime from a tool name. Families keep input-sensitive logic reusable
/// without turning the executor into a second catalog.
#[must_use]
pub fn builtin_effect_resolver_spec(name: &str) -> ToolEffectResolverSpec {
    let resolver_id = match normalize_tool_name(name).as_str() {
        "bash" | "powershell" | "power_shell" | "repl" => "builtin.command",
        // `execute_code` always enters the kernel-hardened workspace sandbox
        // with a bounded timeout/output budget.  It must not inherit the
        // arbitrary host-shell descriptor used by `bash`.
        "execute_code" => "builtin.sandboxed_command",
        // Effect is the strictest effect among the orchestrated sub-calls; the
        // host computes the escalation and this resolver shapes the descriptor.
        "tool_orchestrate" => "builtin.orchestrate",
        "read_file"
        | "read_many"
        | "glob_search"
        | "glob_many"
        | "grep_search"
        | "grep_many"
        | "ast_grep_search"
        | "ast_search"
        | "workspace_snapshot"
        | "tool_batch_readonly"
        | "tool_cache_stats"
        | "mutation_preview"
        | "edit_many_preview"
        | "patch_plan"
        | "checkpoint_list"
        | "checkpoint_diff"
        | "question"
        | "ask_user_question"
        | "tool_search"
        | "background_task_status"
        | "structured_output"
        | "current_time"
        | "get_context_remaining"
        | "testing_permission" => "builtin.readonly",
        "lsp" => "builtin.readonly_process",
        "vision_analyze" => "builtin.readonly_process",
        "write_file"
        | "edit_file"
        | "apply_patch_transaction"
        | "checkpoint_create"
        | "checkpoint_restore"
        | "notebook_edit"
        | "todo_write"
        | "background_task_kill"
        | "config"
        | "enter_plan_mode"
        | "exit_plan_mode" => "builtin.workspace_write",
        "web_fetch" | "web_search" | "remote_trigger" | "send_user_message"
        | "skill_install_plan" => "builtin.network",
        "request_plugin_install"
        | "skill_install_commit"
        | "skill_rollback"
        | "skill_deactivate" => "builtin.external_unknown",
        "skill_status" => "builtin.readonly",
        "sleep" => "builtin.process",
        "list_mcp_resources" | "read_mcp_resource" | "mcp_auth" | "mcp" => {
            "builtin.external_unknown"
        }
        _ => "builtin.unknown",
    };
    ToolEffectResolverSpec {
        resolver_id: resolver_id.to_string(),
        resolver_version: 1,
    }
}

pub(crate) fn normalize_tool_name(value: &str) -> String {
    let value = value.trim().replace('-', "_");
    let chars = value.chars().collect::<Vec<_>>();
    let mut normalized = String::with_capacity(chars.len() + 4);
    for (index, ch) in chars.iter().copied().enumerate() {
        if ch.is_ascii_uppercase() {
            let previous_is_word = index > 0
                && (chars[index - 1].is_ascii_lowercase() || chars[index - 1].is_ascii_digit());
            let starts_word_after_acronym = index > 0
                && chars[index - 1].is_ascii_uppercase()
                && chars.get(index + 1).is_some_and(char::is_ascii_lowercase);
            if (previous_is_word || starts_word_after_acronym) && !normalized.ends_with('_') {
                normalized.push('_');
            }
            normalized.push(ch.to_ascii_lowercase());
        } else {
            normalized.push(ch.to_ascii_lowercase());
        }
    }
    normalized
}

pub(crate) fn permission_mode_from_plugin(value: &str) -> Result<PermissionMode, String> {
    match value {
        "read-only" => Ok(PermissionMode::ReadOnly),
        "workspace-write" => Ok(PermissionMode::WorkspaceWrite),
        "danger-full-access" => Ok(PermissionMode::DangerFullAccess),
        other => Err(format!("unsupported plugin permission: {other}")),
    }
}

#[must_use]
#[allow(clippy::too_many_lines)]
pub fn mvp_tool_specs() -> Vec<ToolSpec> {
    vec![
        ToolSpec {
            name: "bash",
            description: "Execute a shell command in the current workspace. Prefer \
                one-shot multi-step chains (&&, |, tee) over repeated single-step calls: each \
                call is a separate turn of the budget. The shell is the right tool for search, \
                count, filter and transform with small output (grep, find, wc, sort, awk, sed, \
                cut): one chain can locate and summarize in a single call. For reading raw file \
                content, use read_file (a bounded line window) - do not cat large files into \
                context. Output is budgeted: when stdout or stderr exceeds the budget, the \
                visible text is a head+tail preview with an `[output truncated]` marker and the \
                full output is persisted - the response includes `persistedOutputPath`; re-read \
                the needed range via read_file instead of re-running the command. A truncated \
                result is partial evidence; verify it before concluding. For long-running work, set \
                run_in_background=true to detach and return a job id; query it with \
                background_task_status and cancel it with background_task_kill. Do not run \
                interactive commands.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "command": { "type": "string" },
                    "cwd": { "type": "string" },
                    "timeout_ms": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 300000,
                        "description": "Wall-clock timeout in milliseconds (default: no tool-level timeout; maximum: 300000)."
                    },
                    "description": { "type": "string" },
                    "run_in_background": {
                        "type": "boolean",
                        "default": false,
                        "description": "Run detached and return a job id instead of waiting; query it with background_task_status and cancel it with background_task_kill."
                    },
                    "env": {
                        "type": "object",
                        "properties": {
                            "inherit": {
                                "type": "string",
                                "enum": ["safe", "none"],
                                "default": "safe",
                                "description": "safe masks secrets and COWD_* control variables; none inherits nothing. host-secret inheritance is not model-controllable (S-02)."
                            },
                            "exclude": {
                                "type": "array",
                                "items": { "type": "string" },
                                "description": "Host keys to drop regardless of mode."
                            },
                            "set": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "key": { "type": "string" },
                                        "value": { "type": "string" }
                                    },
                                    "required": ["key", "value"],
                                    "additionalProperties": false
                                }
                            }
                        },
                        "additionalProperties": false
                    }
                },
                "required": ["command"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::DangerFullAccess,
        },
        ToolSpec {
            name: "background_task_status",
            description: "Inspect a background job started with run_in_background: command, running/finished state, exit code, elapsed time, byte counts, persistedOutputPath and a head+tail preview. tail=N returns only the last N bytes. Boundary: it reports a job's state and a preview, never its full output - re-read the persisted range with read_file, and it requires that the job was started by run_in_background in this session.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "jobId": { "type": "string" },
                    "tail": { "type": "integer", "minimum": 1, "maximum": 65536 }
                },
                "required": ["jobId"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "background_task_kill",
            description: "Cancel a background job by killing its process group; the terminal state is recorded and surfaced next turn. Boundary: it is terminal for that job - use background_task_status first if you still need its output, and it only reaches jobs started in this session.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "jobId": { "type": "string" }
                },
                "required": ["jobId"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::WorkspaceWrite,
        },
        ToolSpec {
            name: "read_file",
            description: "Read one workspace text file as a bounded line window (offset/limit), or the whole file with complete=true under the file-size safety ceiling. Prefer one substantial window over many small slices: a file that matters is worth reading widely. Boundary: the result is budgeted - on overflow you get head+tail plus truncated:true, nextOffset and a guidance block; continue from nextOffset, or narrow with grep_search first. complete=true is the only mode that yields a verifiable EOF receipt.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "offset": { "type": "integer", "minimum": 0 },
                    "limit": { "type": "integer", "minimum": 1 },
                    "complete": { "type": "boolean", "default": false }
                },
                "required": ["path"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "read_many",
            description: "Read many workspace text files in ONE ordered batch. Items accept exactly the path/offset/limit/complete fields of read_file and run in parallel (bounded by max_concurrency, default the runtime ceiling) with per-item durationMs. Use it when you already know the paths. Boundary: each item is still a bounded window - a whole body only arrives with complete=true under the file-size ceiling, and the batch output is subject to the per-tool result budget, so prefer tool_orchestrate when the paths are not yet known or you must filter/aggregate between reads.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "files": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "path": { "type": "string" },
                                "offset": { "type": "integer", "minimum": 0 },
                                "limit": { "type": "integer", "minimum": 1 },
                                "complete": { "type": "boolean", "default": false }
                            },
                            "required": ["path"],
                            "additionalProperties": false
                        }
                    },
                    "max_concurrency": { "type": "integer", "minimum": 1, "maximum": 42 }
                },
                "required": ["files"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "write_file",
            description: "Create or fully replace a workspace text file. Boundary: it overwrites - use edit_file for a targeted replacement and mutation_preview to dry-run first. Large content is budgeted.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "content": { "type": "string" }
                },
                "required": ["path", "content"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::WorkspaceWrite,
        },
        ToolSpec {
            name: "edit_file",
            description: "Replace an exact string in one workspace file (replace_all for every occurrence). Boundary: the old string must match exactly once unless replace_all is set; the tool fails closed on a miss rather than guessing.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "old_string": { "type": "string" },
                    "new_string": { "type": "string" },
                    "replace_all": { "type": "boolean" }
                },
                "required": ["path", "old_string", "new_string"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::WorkspaceWrite,
        },
        ToolSpec {
            name: "mutation_preview",
            description: "Dry-run multiple text replacements without writing anything, reporting resulting content, replacement counts and hash changes per file. Canonical name for this operation: edit_many_preview and patch_plan are aliases routing to the same handler, kept so existing callers keep working. Use it to validate a multi-file edit before apply_patch_transaction.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "edits": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "path": { "type": "string" },
                                "old_string": { "type": "string" },
                                "new_string": { "type": "string" },
                                "replace_all": { "type": "boolean" }
                            },
                            "required": ["path", "old_string", "new_string"],
                            "additionalProperties": false
                        }
                    }
                },
                "required": ["edits"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "edit_many_preview",
            description: "Alias for mutation_preview.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "edits": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "path": { "type": "string" },
                                "old_string": { "type": "string" },
                                "new_string": { "type": "string" },
                                "replace_all": { "type": "boolean" }
                            },
                            "required": ["path", "old_string", "new_string"],
                            "additionalProperties": false
                        }
                    }
                },
                "required": ["edits"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "patch_plan",
            description: "Alias for mutation_preview.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "edits": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "path": { "type": "string" },
                                "old_string": { "type": "string" },
                                "new_string": { "type": "string" },
                                "replace_all": { "type": "boolean" }
                            },
                            "required": ["path", "old_string", "new_string"],
                            "additionalProperties": false
                        }
                    }
                },
                "required": ["edits"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "apply_patch_transaction",
            description: "Apply a multi-file mutation plan after expected-hash verification, in declaration order under the per-call scope lock. Boundary: a tampered or stale target hash refuses the whole transaction; the effect escalates to the strictest sub-edit, so a write orchestration needs write authorization.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "edits": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "path": { "type": "string" },
                                "old_string": { "type": "string" },
                                "new_string": { "type": "string" },
                                "replace_all": { "type": "boolean" }
                            },
                            "required": ["path", "old_string", "new_string"],
                            "additionalProperties": false
                        }
                    },
                    "expected_hashes": {
                        "type": "object",
                        "additionalProperties": { "type": "string" }
                    }
                },
                "required": ["edits"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::WorkspaceWrite,
        },
        ToolSpec {
            name: "checkpoint_create",
            description: "Create a lightweight workspace checkpoint before risky edits. Boundary: it snapshots the workspace, not the session state, so restore it only to undo workspace content.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "label": { "type": "string" },
                    "paths": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional workspace-relative paths for a bounded checkpoint."
                    }
                },
                "additionalProperties": false
            }),
            required_permission: PermissionMode::WorkspaceWrite,
        },
        ToolSpec {
            name: "checkpoint_list",
            description: "List local workspace checkpoints with their ids and scope. Boundary: local checkpoints only - it never sees checkpoints held by another session or host.",
            input_schema: json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "checkpoint_diff",
            description: "Compare the current workspace against a checkpoint, reporting changed files and hashes. Boundary: it reports differences, never the changed content itself.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "id": { "type": "string" }
                },
                "required": ["id"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "checkpoint_restore",
            description: "Restore files from a local workspace checkpoint. Boundary: it overwrites current content - diff first.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "id": { "type": "string" }
                },
                "required": ["id"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::WorkspaceWrite,
        },
        ToolSpec {
            name: "glob_search",
            description: "Find files by glob pattern, with continuationCursor paging. Use it to turn a where-is-X question into a concrete list before any read. Boundary: scanComplete=false with omissions reports inaccessible or depth-limited subtrees - refine an omitted subtree as a new search rather than assuming the list is complete.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "pattern": { "type": "string" },
                    "include_ignored": { "type": "boolean", "description": "Include hidden and project-ignored paths; workspace protection still applies." },
                    "ignore_patterns": { "type": "array", "items": { "type": "string" }, "description": "Additional exclusion globs relative to the search root." },
                    "path": { "type": "string" },
                    "cursor": { "type": "string", "description": "Opaque query-bound cursor; prefer copying next_request unchanged." }
                },
                "required": ["pattern"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "glob_many",
            description: "Run several independent filename searches in ONE ordered batch, each continuing through its own continuationCursor. Boundary: each scan is bounded by depth, entries, duration and file size - inspect scanComplete/omissions and refine an omitted subtree as a new search; prefer tool_orchestrate when patterns must come from an earlier step.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "patterns": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "pattern": { "type": "string" },
                    "include_ignored": { "type": "boolean", "description": "Include hidden and project-ignored paths; workspace protection still applies." },
                    "ignore_patterns": { "type": "array", "items": { "type": "string" }, "description": "Additional exclusion globs relative to the search root." },
                                "path": { "type": "string" },
                                "cursor": { "type": "string", "description": "Opaque query-bound cursor; prefer copying next_request unchanged." }
                            },
                            "required": ["pattern"],
                            "additionalProperties": false
                        }
                    },
                    "max_concurrency": { "type": "integer", "minimum": 1, "maximum": 42 }
                },
                "required": ["patterns"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "grep_search",
            description: "Locate symbols, text or logic inside large files with a regex, returning exact matching lines with optional context lines. This is the cheapest way to find where to read: locate, then read only the range you need. Boundary: scans are bounded by depth, entries, duration and file size; use glob to narrow the candidate set first, and read the hit's range with read_file(offset=nextOffset) instead of re-scanning.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "pattern": { "type": "string" },
                    "include_ignored": { "type": "boolean", "description": "Include hidden and project-ignored paths; workspace protection still applies." },
                    "ignore_patterns": { "type": "array", "items": { "type": "string" }, "description": "Additional exclusion globs relative to the search root." },
                    "path": { "type": "string" },
                    "glob": { "type": "string" },
                    "output_mode": { "type": "string", "enum": ["content", "files_with_matches", "count"] },
                    "-B": { "type": "integer", "minimum": 0 },
                    "-A": { "type": "integer", "minimum": 0 },
                    "-C": { "type": "integer", "minimum": 0 },
                    "context": { "type": "integer", "minimum": 0 },
                    "-n": { "type": "boolean" },
                    "-i": { "type": "boolean" },
                    "type": { "type": "string" },
                    "head_limit": { "type": "integer", "minimum": 1 },
                    "offset": { "type": "integer", "minimum": 0 },
                    "multiline": { "type": "boolean" }
                    ,"cursor": { "type": "string", "description": "Opaque query-bound cursor; prefer copying next_request unchanged." }
                },
                "required": ["pattern"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "ast_grep_search",
            description: "Regex content search restricted to one language's file extensions, returning path/line/column. Use it to sweep a language's files for a textual pattern while excluding every other language. Boundary: this is NOT a structural matcher - the pattern is a plain regex applied per line, so it cannot express a function named X or a struct with field Y; for arbitrary content use grep_search, and there is no AST tool in this catalog.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "pattern": { "type": "string" },
                    "language": { "type": "string", "description": "rust, python, typescript, javascript, go, java, c, cpp, csharp, ruby, php, shell, sql, toml, yaml, json, markdown" },
                    "path": { "type": "string" },
                    "case_sensitive": { "type": "boolean" },
                    "max_files": { "type": "integer", "minimum": 1, "maximum": 2000 },
                    "max_matches": { "type": "integer", "minimum": 1, "maximum": 500 }
                },
                "required": ["pattern", "language"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "grep_many",
            description: "Run several independent content searches in ONE ordered batch; each entry takes the full grep_search field set and they run in parallel. Boundary: prefer tool_orchestrate when the patterns must be derived from an earlier result - its map step fans out the same way and compute can aggregate the hits without moving them into context.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "searches": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "pattern": { "type": "string" },
                    "include_ignored": { "type": "boolean", "description": "Include hidden and project-ignored paths; workspace protection still applies." },
                    "ignore_patterns": { "type": "array", "items": { "type": "string" }, "description": "Additional exclusion globs relative to the search root." },
                                "path": { "type": "string" },
                                "glob": { "type": "string" },
                                "output_mode": { "type": "string", "enum": ["content", "files_with_matches", "count"] },
                                "-B": { "type": "integer", "minimum": 0 },
                                "-A": { "type": "integer", "minimum": 0 },
                                "-C": { "type": "integer", "minimum": 0 },
                                "context": { "type": "integer", "minimum": 0 },
                                "-n": { "type": "boolean" },
                                "-i": { "type": "boolean" },
                                "type": { "type": "string" },
                                "head_limit": { "type": "integer", "minimum": 1 },
                                "offset": { "type": "integer", "minimum": 0 },
                                "multiline": { "type": "boolean" },
                                "cursor": { "type": "string", "description": "Copy next_request or continuationCursor for this search." }
                            },
                            "required": ["pattern"],
                            "additionalProperties": false
                        }
                    },
                    "max_concurrency": { "type": "integer", "minimum": 1, "maximum": 42 }
                },
                "required": ["searches"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "workspace_snapshot",
            description: "Discover actual workspace and repository roots, verified document entries, and the executable expand/read/search requests available, before guessing paths. Boundary: directory and file pages are live-filesystem snapshots with their own continuation - they change as the workspace changes, so do not cache a listing across a mutation.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "include_git": { "type": "boolean" },
                    "include_files": { "type": "boolean" },
                    "roots": { "type": "array", "minItems": 1, "maxItems": 32, "items": { "type": "string" } },
                    "query": { "type": "string" },
                    "cursor": { "type": "string" },
                    "page_size": { "type": "integer", "minimum": 1, "maximum": 100 },
                    "include_ignored": { "type": "boolean" },
                    "ignore_patterns": { "type": "array", "items": { "type": "string" } },
                    "max_files": { "type": "integer", "minimum": 1, "maximum": 5000 }
                },
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "tool_batch_readonly",
            description: "Execute several approved read-only tool calls in ONE ordered batch, in parallel, with per-item status and durationMs. Boundary: read-only tools only - writes are refused - and it is the flat form of tool_orchestrate, so prefer tool_orchestrate when you need map/filter/compute between calls.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "calls": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": { "type": "string" },
                                "input": { "type": "object" }
                            },
                            "required": ["name", "input"],
                            "additionalProperties": false
                        }
                    },
                    "max_concurrency": { "type": "integer", "minimum": 1, "maximum": 42 }
                },
                "required": ["calls"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "tool_orchestrate",
            description: "Run a bounded PROGRAM of many tool calls in ONE provider round: one instruction reaches its result without a round trip per step. This is the shortest path whenever the work is more than a single step - locate, read, aggregate and report in one call instead of one call per round. Bounds: <=50 steps, <=400 sub-calls in total, map over <=200 items, repeat <=25 rounds. Sub-calls run in declaration order and share values through `$<step_id>` references inside input objects; write a literal $ as $$. Step shapes, exactly as the input schema accepts them: {op:call,id,tool,input} one sub-call. {op:batch,id,calls:[{tool,input,as?}]} parallel fan-out (the same engine as read_many/grep_many). {op:map,id,over:$list,tool,input,field?} runs `tool` once per item of a prior list/collect result; `field` extracts that field from each item before it is passed as the item. {op:filter,id,over:$list,field,contains?,equals?} keeps the items whose `field` contains or equals a value. {op:collect,id,over:$list,agg,field?,separator?} aggregates a list, where agg is count|first|last|join|sum (join takes `separator`, default a single space). {op:compute,id,language,code,input?} runs code in the kernel-hardened read-only network-off sandbox over the data the program already holds and returns only stdout/stderr - use it to count, aggregate or compare across many files instead of moving their bodies into context. {op:when,id,if:{ref:$id,present?,equals?,contains?,non_empty?},tool,input?} runs one sub-call only when the condition holds. {op:repeat,id,max,while:{ref:$id,...},tool,input?} re-runs one sub-call while the condition holds, at most max times. Vocabulary: any catalog tool may be dispatched - read_file/read_many/glob_search/glob_many/grep_search/grep_many/ast_grep_search/workspace_snapshot to locate and read; execute_code/shell to reduce; write_file/edit_file/apply_patch_transaction to mutate, which needs write authorization; todo_write/checkpoint_*/skill_*/lsp/web_search/web_fetch/mcp to govern and integrate. tool_orchestrate itself may never be nested. Boundaries: every sub-call is authorized independently against the session tool allow-list, so an unauthorized tool fails closed naming that tool - the program never grants extra permission, and a write-requiring program is refused in a read-only session. Output is one JSON object: the `return` expression's value plus a per-step trace (op, tool, status, durationMs, error). Use `return` to select only the digest you need, because the whole result is subject to the tool-result budget.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "steps": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 50,
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                                "op": {
                                    "type": "string",
                                    "enum": ["call", "batch", "map", "filter", "collect", "compute", "when", "repeat"],
                                    "description": "Step kind. call/batch/map/filter/collect/compute/when/repeat."
                                },
                                "tool": { "type": "string" },
                                "input": { "type": "object" },
                                "calls": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "tool": { "type": "string" },
                                            "input": { "type": "object" },
                                            "as": { "type": "string" }
                                        },
                                        "required": ["tool"]
                                    }
                                },
                                "over": { "type": "string", "description": "$<step_id> reference to a list." },
                                "field": { "type": "string" },
                                "contains": { "type": "string" },
                                "equals": {},
                                "agg": {
                                    "type": "string",
                                    "enum": ["count", "first", "last", "join", "sum"]
                                },
                                "separator": { "type": "string" },
                                "language": { "type": "string" },
                                "code": { "type": "string" },
                                "max": { "type": "integer", "minimum": 1, "maximum": 25 },
                                "if": {
                                        "type": "object",
                                        "properties": {
                                            "ref": { "type": "string" },
                                            "present": { "type": "boolean" },
                                            "equals": {},
                                            "contains": { "type": "string" },
                                            "non_empty": { "type": "boolean" }
                                        },
                                        "required": ["ref"]
                                    },
                                "while": {
                                        "type": "object",
                                        "properties": {
                                            "ref": { "type": "string" },
                                            "present": { "type": "boolean" },
                                            "equals": {},
                                            "contains": { "type": "string" },
                                            "non_empty": { "type": "boolean" }
                                        },
                                        "required": ["ref"]
                                    }
                            },
                            "required": ["id", "op"]
                        }
                    },
                    "return": { "type": "string", "description": "$<step_id> to return." }
                },
                "required": ["steps"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "tool_cache_stats",
            description: "Report in-process read-only cache hit, miss, invalidation and entry counts. Use it to diagnose whether reads are being served from cache.",
            input_schema: json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "web_fetch",
            description: "Fetch a URL and answer a prompt about it, with allowed/blocked domain scoping. Boundary: the answer is only as good as the fetched text - cite the URL, and prefer web_search first to find the right one.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "url": { "type": "string", "format": "uri" },
                    "prompt": { "type": "string" },
                    "allowed_domains": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional per-call narrowing; cannot widen the configured network domain policy."
                    },
                    "blocked_domains": {
                        "type": "array",
                        "items": { "type": "string" }
                    }
                },
                "required": ["url", "prompt"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "web_search",
            description: "Federated web search across public sources with intent routing, deduplication, source receipts and cited results. Use it for facts that are not in the workspace. Boundary: results carry source receipts and any external claim must be cited; the tool does not fetch page bodies.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string", "minLength": 2 },
                    "intent": {
                        "type": "string",
                        "enum": ["auto", "general", "code", "research", "knowledge"],
                        "default": "auto"
                    },
                    "depth": {
                        "type": "string",
                        "enum": ["quick", "standard", "deep"],
                        "default": "standard"
                    },
                    "max_results": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 20
                    },
                    "locale": { "type": "string" },
                    "allowed_domains": {
                        "type": "array",
                        "items": { "type": "string" }
                    },
                    "blocked_domains": {
                        "type": "array",
                        "items": { "type": "string" }
                    },
                    "recency": {
                        "type": "string",
                        "enum": ["any", "day", "week", "month", "year"],
                        "default": "any",
                        "description": "When set, results are re-ranked by publication freshness and filtered to the requested window when possible."
                    }
                },
                "required": ["query"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "todo_write",
            description: "Maintain the structured task list with priorities and status. Use it to make multi-step work auditable and resumable. Boundary: it is the plan of record - keep it short and current, not a log.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "todos": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "content": { "type": "string" },
                                "activeForm": { "type": "string" },
                                "status": {
                                    "type": "string",
                                    "enum": ["pending", "in_progress", "completed"]
                                },
                                "priority": {
                                    "type": "string",
                                    "enum": ["low", "medium", "high", "critical"],
                                    "description": "Task priority level (default: medium)"
                                }
                            },
                            "required": ["content", "activeForm", "status"],
                            "additionalProperties": false
                        }
                    }
                },
                "required": ["todos"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::WorkspaceWrite,
        },
        ToolSpec {
            name: "question",
            description: "Ask the user a clarifying question with optional choices and wait. Alias for ask_user_question (same question/options input), kept for existing callers. Boundary: it blocks the turn until a reply - use send_user_message when you do not need an answer.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "question": { "type": "string", "description": "The question to ask" },
                    "options": { "type": "array", "items": { "type": "string" } }
                },
                "required": ["question"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "current_time",
            description: "Return the current UTC time and local timezone as a bounded RFC3339 timestamp. Boundary: it is the Runtime clock, not a workspace fact - prefer the evidence in a tool result when both exist.",
            input_schema: json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "get_context_remaining",
            description: "Report the active conversation's context utilisation and remaining token budget. Use it to decide whether to compact, narrow scope, or stop reading.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "detail": {
                        "type": "string",
                        "enum": ["summary", "full"],
                        "default": "summary"
                    }
                },
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "request_plugin_install",
            description: "Refuses by design: plugin installation is an operator control-plane operation, never a model action. Present so a request fails closed with a clear reason instead of being silently ignored.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "plugin_id": { "type": "string" }
                },
                "required": ["plugin_id"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::DangerFullAccess,
        },
        ToolSpec {
            name: "skill_install_plan",
            description: "Acquire a local or GitHub skill as inert content, inspect every bounded file, and return the canonical SHA-256 tree digest plus blockers/warnings without activating anything. Use it before any install.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "source": { "type": "string", "minLength": 1 }
                },
                "required": ["source"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "skill_install_commit",
            description: "Re-acquire a previously planned skill, require the exact expected SHA-256 tree digest, enforce supply-chain blockers, atomically activate an immutable revision and emit a provenance receipt. Boundary: it never executes packaged code, and a digest mismatch refuses the install.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "source": { "type": "string", "minLength": 1 },
                    "expected_digest": { "type": "string", "pattern": "^sha256:[0-9a-f]{64}$" },
                    "allow_warnings": { "type": "boolean", "default": false }
                },
                "required": ["source", "expected_digest"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::DangerFullAccess,
        },
        ToolSpec {
            name: "skill_status",
            description: "Read the active pointer, immutable revision history and install receipts for one skill. Boundary: it is a query for one skill and needs its id - use skill_install_plan first when the skill is not installed yet.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "skill_id": { "type": "string", "minLength": 1 }
                },
                "required": ["skill_id"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "skill_rollback",
            description: "Atomically reactivate an existing immutable skill revision after recomputing and verifying its canonical digest. Boundary: it can only move to a revision that already exists, so it never re-acquires remote content, and a digest mismatch refuses.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "skill_id": { "type": "string", "minLength": 1 },
                    "revision": { "type": "string", "pattern": "^sha256:[0-9a-f]{64}$" }
                },
                "required": ["skill_id", "revision"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::DangerFullAccess,
        },
        ToolSpec {
            name: "skill_deactivate",
            description: "Atomically remove a skill's active pointer while retaining immutable revisions and receipts for audit and rollback. Boundary: it deactivates, never deletes - the revisions stay rollback-able via skill_rollback.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "skill_id": { "type": "string", "minLength": 1 }
                },
                "required": ["skill_id"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::DangerFullAccess,
        },
        ToolSpec {
            name: "tool_search",
            description: "Find a deferred or specialized tool by exact name or keywords and receive its descriptors. Boundary: activation applies to the next request in the SAME turn and it changes the tool set, so search once with a focused query rather than probing.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "max_results": { "type": "integer", "minimum": 1 }
                },
                "required": ["query"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "notebook_edit",
            description: "Replace, insert or delete a Jupyter notebook cell. Boundary: it edits cell source only - it does not execute the notebook.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "notebook_path": { "type": "string" },
                    "cell_id": { "type": "string" },
                    "new_source": { "type": "string" },
                    "cell_type": { "type": "string", "enum": ["code", "markdown"] },
                    "edit_mode": { "type": "string", "enum": ["replace", "insert", "delete"] }
                },
                "required": ["notebook_path"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::WorkspaceWrite,
        },
        ToolSpec {
            name: "sleep",
            description: "Wait without holding a shell process. Boundary: it only paces an external dependency and burns a round of the turn budget, so prefer a background job or a longer tool timeout.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "duration_ms": { "type": "integer", "minimum": 0 }
                },
                "required": ["duration_ms"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "send_user_message",
            description: "Send a message to the user without waiting for a reply. Use it for progress and decisions; use ask_user_question when you need an answer.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "message": { "type": "string" },
                    "attachments": {
                        "type": "array",
                        "items": { "type": "string" }
                    },
                    "status": {
                        "type": "string",
                        "enum": ["normal", "proactive"]
                    }
                },
                "required": ["message", "status"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "config",
            description: "Read or set a Cowd setting. Boundary: it changes local configuration and needs write authorization; operator-owned settings are refused.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "setting": { "type": "string" },
                    "value": {
                        "type": ["string", "boolean", "number"]
                    }
                },
                "required": ["setting"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::WorkspaceWrite,
        },
        ToolSpec {
            name: "enter_plan_mode",
            description: "Enable a worktree-local planning-mode override, remembering the previous local setting so exit_plan_mode can restore it. Boundary: it only affects this worktree, not other sessions or hosts.",
            input_schema: json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
            required_permission: PermissionMode::WorkspaceWrite,
        },
        ToolSpec {
            name: "exit_plan_mode",
            description: "Restore or clear the planning-mode override created by enter_plan_mode. Boundary: it requires a matching enter_plan_mode in this worktree; with none it fails closed rather than guessing the previous state.",
            input_schema: json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
            required_permission: PermissionMode::WorkspaceWrite,
        },
        ToolSpec {
            name: "structured_output",
            description: "Return the final answer in the requested structured shape. Boundary: it is a presentation contract, not evidence - committed tool receipts still carry the facts.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": true
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "repl",
            description: "Run code in an interactive-style subprocess with your own timeout. This is the stateful, full-access sibling of execute_code: it can write files and touch the network, so it needs full-access authorization. Boundary: prefer execute_code for pure reduction - it is read-only, network-off and deterministic.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "code": { "type": "string" },
                    "language": { "type": "string" },
                    "timeout_ms": { "type": "integer", "minimum": 1 }
                },
                "required": ["code", "language"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::DangerFullAccess,
        },
        ToolSpec {
            name: "power_shell",
            description: "Run a PowerShell command (Windows hosts) with an optional timeout and background detach. Same boundary as bash: full-access authorization.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "command": { "type": "string" },
                    "timeout_ms": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 300000,
                        "description": "Wall-clock timeout in milliseconds."
                    },
                    "description": { "type": "string" },
                    "run_in_background": { "type": "boolean" }
                },
                "required": ["command"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::DangerFullAccess,
        },
        ToolSpec {
            name: "ask_user_question",
            description: "Ask the user a question with optional choices and block until they answer. Boundary: it stops the turn - use send_user_message when a reply is not required.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "question": { "type": "string" },
                    "options": {
                        "type": "array",
                        "items": { "type": "string" }
                    }
                },
                "required": ["question"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "lsp",
            description: "Query a connected language server for symbols, references, hover or diagnostics at a position. Boundary: it needs a running server for that language, and it is only active when the experimental LSP surface is enabled.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": ["symbols", "references", "diagnostics", "definition", "hover"] },
                    "path": { "type": "string" },
                    "line": { "type": "integer", "minimum": 0 },
                    "character": { "type": "integer", "minimum": 0 },
                    "query": { "type": "string" }
                },
                "required": ["action"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "list_mcp_resources",
            description: "List the resources published by connected MCP servers. Boundary: it lists resources, not tools - use mcp to call a tool, and a server must be configured and reachable to appear.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "server": { "type": "string" }
                },
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "read_mcp_resource",
            description: "Read one MCP resource by server and uri. Boundary: the server must be configured and reachable, and the uri must be one that server actually published - prefer list_mcp_resources first.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "server": { "type": "string" },
                    "uri": { "type": "string" }
                },
                "required": ["uri"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "mcp_auth",
            description: "Authenticate with an MCP server that requires OAuth or credentials. Operator action, not a task step: use it only when a server reports an auth failure and the credential can be obtained without human input. Boundary: it never stores or prints a secret.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "server": { "type": "string" }
                },
                "required": ["server"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::DangerFullAccess,
        },
        ToolSpec {
            name: "remote_trigger",
            description: "POST to an arbitrary remote endpoint or webhook. Boundary: network egress and an operator-supplied URL are required - it is a control-plane action, not a task tool, and needs full-access authorization.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "url": { "type": "string" },
                    "method": { "type": "string", "enum": ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"] },
                    "headers": { "type": "object" },
                    "body": { "type": "string" },
                    "timeout_ms": { "type": "integer", "minimum": 1, "maximum": 300000 }
                },
                "required": ["url"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::DangerFullAccess,
        },
        ToolSpec {
            name: "mcp",
            description: "Execute a tool exposed by a connected external MCP server, or list/read its resources. This is the only bridge to non-local capability: it is heavier than a local primitive and needs the server configured, so prefer local tools for anything the catalog already covers. Boundary: the server must be reachable and authenticated, and arguments pass through to it unvalidated.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "server": { "type": "string" },
                    "tool": { "type": "string" },
                    "arguments": { "type": "object" }
                },
                "required": ["server", "tool"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::DangerFullAccess,
        },
        ToolSpec {
            name: "testing_permission",
            description: "Test-only probe for permission enforcement. It has no production purpose and refuses outside a test.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string" }
                },
                "required": ["action"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::DangerFullAccess,
        },
        // 3B-4: Built-in vision analysis tool (requires multimodal LLM support)
        ToolSpec {
            name: "vision_analyze",
            description: "Prepare a local image for multimodal analysis: validate and encode it, and the runtime attaches it as a structured vision input on the next model call. Boundary: local images only, and it consumes one model round.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "image_path": {
                        "type": "string",
                        "description": "Path to the image file to analyze (PNG, JPG, GIF, WebP supported)"
                    },
                    "prompt": {
                        "type": "string",
                        "description": "What to analyze or describe about the image"
                    },
                    "detail": {
                        "type": "string",
                        "enum": ["low", "high", "auto"],
                        "description": "Level of detail for image analysis (default: auto)"
                    }
                },
                "required": ["image_path", "prompt"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "execute_code",
            description: "Deterministic reduction OUTSIDE the model. Runs python/javascript/bash/ruby/lua in a kernel-hardened, read-only, network-off sandbox and returns ONLY stdout/stderr (30s cap, nothing persisted). This is the tool for work that would otherwise mean pulling many files or many lines into context: count, aggregate, filter, compare, compute. Boundary: it cannot read the workspace itself - feed it data from read_file/read_many/grep_search, or dispatch it as tool_orchestrate's compute step where the program already holds the data.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "language": {
                        "type": "string",
                        "enum": ["python", "javascript", "bash", "ruby", "lua"],
                        "description": "Programming language to execute"
                    },
                    "code": {
                        "type": "string",
                        "description": "Code to execute. Use console.log/print to output results. Only stdout enters context."
                    }
                },
                "required": ["language", "code"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
    ]
}
#[cfg(test)]
mod tests {
    use super::{builtin_effect_resolver_spec, mvp_tool_specs, normalize_tool_name};
    use crate::permissions::PermissionMode;

    #[test]
    fn read_file_guides_bounded_window_and_deterministic_pagination() {
        let specs = mvp_tool_specs();
        let read_file = specs
            .iter()
            .find(|spec| spec.name == "read_file")
            .expect("read_file spec");
        let grep_search = specs
            .iter()
            .find(|spec| spec.name == "grep_search")
            .expect("grep_search spec");

        assert!(read_file.description.contains("bounded"));
        // The doc must push toward ONE substantial window rather than many small
        // slices, and must name the recovery path and the EOF mode.
        assert!(read_file.description.contains("substantial window"));
        assert!(read_file.description.contains("nextOffset"));
        assert!(read_file.description.contains("complete=true"));
        // The key boundary must be stated: the result is budgeted and overflow
        // is announced, never silent.
        assert!(read_file.description.contains("budgeted"));
        assert!(read_file.description.contains("truncated:true"));
        assert_eq!(
            read_file.input_schema["properties"]["complete"]["type"],
            "boolean"
        );
        assert!(grep_search.description.contains("locator")
            || grep_search.description.contains("Locate"));
        // grep must steer the model toward locate-then-read, not line-by-line.
        assert!(grep_search.description.contains("locate"));
    }

    #[test]
    fn bash_description_guides_small_output_shell_and_read_file_for_content() {
        let specs = mvp_tool_specs();
        let bash = specs
            .iter()
            .find(|spec| spec.name == "bash")
            .expect("bash spec");

        // Behaviour disclosure must stay truthful (09 IV.4).
        assert!(bash.description.contains("persistedOutputPath"));
        assert!(bash.description.contains("[output truncated]"));
        // D1: the shell is for small-output search/aggregate, and raw content
        // reads go through read_file instead of `cat` so the model stops
        // iterating small tools. The old blanket bash-forbid must be gone.
        assert!(bash.description.contains("read_file"));
        assert!(bash.description.contains("small output"));
        assert!(!bash
            .description
            .contains("Prefer grep_search/read_file over bash"));
    }

    #[test]
    fn camel_case_tool_names_share_the_canonical_effect_namespace() {
        assert_eq!(normalize_tool_name("tool_search"), "tool_search");
        assert_eq!(normalize_tool_name("web_search"), "web_search");
        assert_eq!(normalize_tool_name("notebook_edit"), "notebook_edit");
        assert_eq!(normalize_tool_name("MCPAuth"), "mcp_auth");
        assert_eq!(
            builtin_effect_resolver_spec("tool_search").resolver_id,
            "builtin.readonly"
        );
        assert_eq!(
            builtin_effect_resolver_spec("web_search").resolver_id,
            "builtin.network"
        );
        assert_eq!(
            builtin_effect_resolver_spec("execute_code").resolver_id,
            "builtin.sandboxed_command"
        );
        assert_eq!(
            builtin_effect_resolver_spec("bash").resolver_id,
            "builtin.command"
        );
    }

    #[test]
    fn every_builtin_tool_declares_a_concrete_effect_resolver() {
        let unknown = mvp_tool_specs()
            .into_iter()
            .filter(|spec| builtin_effect_resolver_spec(spec.name).resolver_id == "builtin.unknown")
            .map(|spec| spec.name)
            .collect::<Vec<_>>();
        assert!(
            unknown.is_empty(),
            "builtin tools without an effect resolver: {unknown:?}"
        );
    }

    #[test]
    fn every_builtin_tool_uses_one_snake_case_catalog_identity() {
        let invalid = mvp_tool_specs()
            .into_iter()
            .map(|spec| spec.name)
            .filter(|name| {
                name.is_empty()
                    || name.starts_with('_')
                    || name.ends_with('_')
                    || name.contains("__")
                    || name.chars().any(|character| {
                        !(character.is_ascii_lowercase()
                            || character.is_ascii_digit()
                            || character == '_')
                    })
            })
            .collect::<Vec<_>>();
        assert!(
            invalid.is_empty(),
            "builtin tools with non-snake-case identities: {invalid:?}"
        );
    }

    #[test]
    fn skill_supply_chain_tools_separate_review_from_mutation() {
        let specs = mvp_tool_specs();
        let plan = specs
            .iter()
            .find(|spec| spec.name == "skill_install_plan")
            .expect("plan tool");
        let commit = specs
            .iter()
            .find(|spec| spec.name == "skill_install_commit")
            .expect("commit tool");
        assert_eq!(plan.required_permission, PermissionMode::ReadOnly);
        assert_eq!(commit.required_permission, PermissionMode::DangerFullAccess);
        assert_eq!(
            builtin_effect_resolver_spec(plan.name).resolver_id,
            "builtin.network"
        );
        assert_eq!(
            builtin_effect_resolver_spec(commit.name).resolver_id,
            "builtin.external_unknown"
        );
    }

    #[test]
    fn shell_timeout_contract_uses_explicit_milliseconds_only() {
        for name in ["bash", "power_shell"] {
            let spec = mvp_tool_specs()
                .into_iter()
                .find(|spec| spec.name == name)
                .expect("shell spec");
            let properties = spec.input_schema["properties"]
                .as_object()
                .expect("properties");
            assert!(properties.contains_key("timeout_ms"));
            assert!(!properties.contains_key("timeout"));
        }
    }
}

/// Every tool's documentation must be a usable specification, not a label.
///
/// The model only ever sees this string, so a thin or wrong description is a
/// capability loss that no test would otherwise catch. Three rules, all
/// mechanical:
///
/// 1. **Non-trivial tools must disclose their boundary.** A description with no
///    limit, permission or failure condition tells the model nothing about
///    where the tool stops being the right choice. Aliases and one-line
///    pass-throughs are exempt because they defer to their canonical name.
/// 2. **No false claim may survive.** `tool_orchestrate` used to describe itself
///    as read-only after its vocabulary was widened to the whole catalog, and
///    `ast_grep_search` claimed to locate code constructs while being a plain
///    per-line regex. Both are now pinned.
/// 3. **The orchestration primitive must document what it dispatches.** It is
///    the only way one instruction drives many calls, so its bounds and
///    vocabulary belong in the description, not in the model's imagination.
#[test]
fn every_tool_documentation_states_power_and_boundary_without_false_claims() {
    let specs = mvp_tool_specs();
    let by_name = |name: &str| {
        specs
            .iter()
            .find(|spec| spec.name == name)
            .unwrap_or_else(|| panic!("{name} spec is missing"))
    };
    // Documented aliases: they intentionally defer to their canonical tool.
    const ALIASES: &[&str] = &[
        "edit_many_preview",
        "patch_plan",
        "question",
    ];

    for spec in &specs {
        if ALIASES.contains(&spec.name) {
            continue;
        }
        assert!(
            spec.description.len() >= 80,
            "`{}` documentation is too thin to be a specification ({} chars): {}",
            spec.name,
            spec.description.len(),
            spec.description
        );
        let lowered = spec.description.to_lowercase();
        assert!(
            lowered.contains("boundary")
                || lowered.contains("prefer")
                || lowered.contains("use it")
                || lowered.contains("note")
                || lowered.contains("fails closed")
                || lowered.contains("refuses")
                || lowered.contains("needs ")
                || lowered.contains("requires"),
            "`{}` never states where it stops being the right choice: {}",
            spec.name,
            spec.description
        );
    }

    // Rule 2: false claims.
    let orchestrate = by_name("tool_orchestrate");
    assert!(
        !orchestrate.description.contains("read-only program"),
        "tool_orchestrate dispatches the whole catalog, not a read-only subset: {}",
        orchestrate.description
    );
    let ast_grep = by_name("ast_grep_search");
    assert!(
        !ast_grep.description.to_lowercase().contains("code construct"),
        "ast_grep_search is a per-line regex, not a structural matcher: {}",
        ast_grep.description
    );
    assert!(
        ast_grep.description.contains("NOT a structural matcher"),
        "ast_grep_search must say what it is not: {}",
        ast_grep.description
    );

    // Rule 3: the orchestration spec must carry bounds and vocabulary.
    for required in [
        "400 sub-calls",
        "50 steps",
        "map",
        "compute",
        "Vocabulary",
        "allow-list",
    ] {
        assert!(
            orchestrate.description.contains(required),
            "tool_orchestrate documentation must cover `{required}`: {}",
            orchestrate.description
        );
    }

    // The two reads must stay clearly different: workspace bytes vs canonical
    // Runtime references. Conflating them is how a model ends up reading a
    // file with the wrong tool.
    let read_file = by_name("read_file");
    assert!(read_file.description.contains("workspace text file"));
    assert!(read_file.description.contains("complete=true"));
}

/// The orchestration specification must match its input schema exactly.
///
/// The previous hand-written description claimed `{op:"filter",...op,value}`,
/// `{op:"collect",...fields,as,limit}` and `{op:"when",...,then:[...],
/// otherwise:[...]}` - none of which exist. The real fields are
/// `filter{field,contains|equals}`, `collect{agg,field,separator}` and
/// `when{if,tool,input}`, and `repeat{max,while,tool,input}` was missing
/// entirely. A model that trusts the doc writes a program the parser rejects,
/// so every op's real field name is pinned here against the schema itself.
#[test]
fn orchestrate_documentation_matches_its_own_input_schema() {
    let specs = mvp_tool_specs();
    let spec = specs
        .iter()
        .find(|spec| spec.name == "tool_orchestrate")
        .expect("tool_orchestrate spec");
    let schema = spec
        .input_schema
        .get("properties")
        .and_then(|properties| properties.get("steps"))
        .and_then(|steps| steps.get("items"))
        .and_then(|items| items.get("properties"))
        .expect("orchestrate steps item schema");
    let doc = spec.description;

    // Every op the schema discriminates on must be named in the doc.
    let ops = schema
        .get("op")
        .and_then(|op| op.get("enum"))
        .and_then(serde_json::Value::as_array)
        .expect("op enum");
    let op_names: Vec<&str> = ops.iter().filter_map(serde_json::Value::as_str).collect();
    assert!(!op_names.is_empty(), "the op enum must be closed");
    for op in &op_names {
        assert!(
            doc.contains(&format!("op:{op}")),
            "op `{op}` exists in the schema but the doc never shows its shape"
        );
    }

    // The doc must not invent an op the schema does not have.
    for invented in ["op:fork", "op:spawn", "op:pipeline", "op:delegate"] {
        assert!(
            !doc.contains(invented),
            "the doc describes a step that does not exist: {invented}"
        );
    }

    // Real per-op field names, as parsed by `StepKind`.
    for required in [
        // call / batch
        "tool",
        "input",
        "as?",
        // map / filter / collect
        "over",
        "field?",
        "contains?",
        "equals?",
        "agg",
        "separator?",
        // compute
        "language",
        "code",
        // when / repeat
        "if:{ref:",
        "max",
        "while:{ref:",
    ] {
        assert!(
            doc.contains(required),
            "the orchestration doc must state the real field `{required}`"
        );
    }

    // Bounds must match the constants in `orchestrate.rs`.
    for bound in ["50 steps", "400 sub-calls", "200 items", "25 rounds"] {
        assert!(
            doc.contains(bound),
            "the orchestration doc must state its bound `{bound}`"
        );
    }
}

/// The catalog-side primitive behind provider tool exposure.
///
/// Kept beside the catalog so the determinism contract is tested against the
/// same input the provider side consumes; the provider layer applies the same
/// filter to its own compiled definitions.
#[cfg(test)]
pub(crate) fn tool_definitions_for_exposure_filter(
    specs: &[ToolSpec],
    active: &std::collections::BTreeSet<String>,
) -> Vec<ToolSpec> {
    specs
        .iter()
        .filter(|spec| active.contains(spec.name))
        .cloned()
        .collect()
}

#[cfg(test)]
mod cache_determinism_tests {
    use super::*;

/// PLAN A3: the tool catalog must serialize to identical bytes every time.
///
/// `tool_definitions_for_exposure` filters the catalog into the array that the
/// provider request carries, and `apply_inline_cache_breakpoints` puts a
/// `cache_control` marker on the LAST tool definition - so the whole tool array
/// is inside cache breakpoint 2. If that array's bytes move, the provider's
/// cache is invalidated from the tools onward, i.e. every message after it.
///
/// The array is filtered from a `BTreeSet` of active ids, which is already
/// deterministic, but nothing enforced it. This pins the three properties that
/// make the array cache-safe:
///
/// 1. the same input set yields the same order and the same bytes;
/// 2. filtering is a pure function of the active set (adding one id appends one
///    entry and leaves the prefix untouched) - so incremental activation cannot
///    rewrite what came before;
/// 3. the catalog contains no duplicate names (a duplicate would make the
///    filter order dependent on the catalog, which is not keyed by anything).
#[test]
fn the_tool_catalog_serializes_deterministically_for_provider_caching() {
    let specs = mvp_tool_specs();

    // (3) no duplicate tool names anywhere in the catalog.
    let mut names = specs.iter().map(|spec| spec.name).collect::<Vec<_>>();
    let total = names.len();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), total, "the catalog must not contain duplicate names");

    // (1) a fixed input set always yields the same bytes. This is what lets the
    // provider cache key on the tools array at all.
    let active = ["read_file", "grep_search", "tool_orchestrate", "glob_search"]
        .into_iter()
        .map(str::to_owned)
        .collect::<std::collections::BTreeSet<_>>();
    let render = |active: &std::collections::BTreeSet<String>| -> String {
        super::tool_definitions_for_exposure_filter(&specs, active)
            .into_iter()
            .map(|spec| {
                // `ToolSpec` is not Serialize, so compare the two things that
                // actually reach the wire: the ordered names and each input
                // schema serialized deterministically.
                format!(
                    "{}:{}",
                    spec.name,
                    serde_json::to_string(&spec.input_schema).unwrap_or_default()
                )
            })
            .collect::<Vec<_>>()
            .join("|")
    };
    let first = render(&active);
    for _ in 0..8 {
        assert_eq!(first, render(&active), "the filtered catalog must be byte-stable");
    }

    // (2) growing the active set appends; the shared prefix is untouched.
    let mut wider = active.clone();
    wider.insert("execute_code".to_owned());
    let wider_bytes = render(&wider);
    let shared = first
        .as_bytes()
        .iter()
        .zip(wider_bytes.as_bytes())
        .take_while(|(left, right)| left == right)
        .count();
    assert!(
        shared >= first.len().saturating_sub(64),
        "adding an entry must not rewrite the stable prefix: shared {shared} of {} bytes",
        first.len()
    );
    assert_eq!(
        super::tool_definitions_for_exposure_filter(&specs, &active).len(),
        4,
        "the filter is exactly the active set"
    );
    assert_eq!(
        super::tool_definitions_for_exposure_filter(&specs, &wider).len(),
        5,
        "one more active id yields exactly one more entry"
    );
}
}
