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
        "bash" | "powershell" | "power_shell" | "repl" | "execute_code" => "builtin.command",
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
        | "config"
        | "enter_plan_mode"
        | "exit_plan_mode" => "builtin.workspace_write",
        "web_fetch" | "web_search" | "remote_trigger" | "send_user_message" => "builtin.network",
        "request_plugin_install" => "builtin.external_unknown",
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
            description: "Execute a shell command in the current workspace.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "command": { "type": "string" },
                    "cwd": { "type": "string" },
                    "timeout": { "type": "integer", "minimum": 1 },
                    "description": { "type": "string" },
                    "dangerouslyDisableSandbox": { "type": "boolean" },
                    "isolateNetwork": { "type": "boolean" },
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
            name: "read_file",
            description: "Read a bounded line window from a workspace text file. For large files, use grep_search first to locate relevant symbols or logic, then read only the matching region with explicit offset and limit. Do not sequentially scan a large file when search can answer the question.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "offset": { "type": "integer", "minimum": 0 },
                    "limit": { "type": "integer", "minimum": 1 }
                },
                "required": ["path"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "read_many",
            description: "Read multiple text files from the workspace in one ordered batch.",
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
                                "limit": { "type": "integer", "minimum": 1 }
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
            description: "Write a text file in the workspace.",
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
            description: "Replace text in a workspace file.",
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
            description: "Preview multiple text replacements without writing files.",
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
            description: "Alias for mutation_preview optimized for multi-edit preflight.",
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
            description: "Build a structured preflight plan for multiple text replacements.",
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
            description: "Apply a multi-file mutation plan after expected-hash verification.",
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
            description: "Create a lightweight workspace checkpoint before risky edits.",
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
            description: "List local workspace checkpoints.",
            input_schema: json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "checkpoint_diff",
            description: "Compare the current workspace against a checkpoint.",
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
            description: "Restore files from a local workspace checkpoint.",
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
            description: "Find files by glob pattern.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "pattern": { "type": "string" },
                    "path": { "type": "string" }
                },
                "required": ["pattern"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "glob_many",
            description: "Run multiple glob searches in one ordered read-only batch.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "patterns": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "pattern": { "type": "string" },
                                "path": { "type": "string" }
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
            description: "Preferred locator for symbols, text, or logic in large files. Search workspace file contents with a regex and return exact matching lines with optional context; use it before bounded read_file calls to avoid expensive full-file scans.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "pattern": { "type": "string" },
                    "path": { "type": "string" },
                    "glob": { "type": "string" },
                    "output_mode": { "type": "string" },
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
                },
                "required": ["pattern"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "ast_grep_search",
            description: "Locate code constructs across language-scoped source files. Searches file contents under the workspace with a regex, filtered by language extension; returns matching lines with path/line/column. Use before read_file to find definitions and call sites.",
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
            description: "Run multiple regex searches in one ordered read-only batch.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "searches": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "pattern": { "type": "string" },
                                "path": { "type": "string" },
                                "glob": { "type": "string" },
                                "output_mode": { "type": "string" },
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
            description: "Collect a compact read-only snapshot of the current workspace state.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "include_git": { "type": "boolean" },
                    "include_files": { "type": "boolean" },
                    "roots": { "type": "array", "items": { "type": "string" } },
                    "max_files": { "type": "integer", "minimum": 1, "maximum": 5000 }
                },
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "tool_batch_readonly",
            description: "Execute multiple approved read-only tool calls in one ordered batch.",
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
            name: "tool_cache_stats",
            description: "Report in-process read-only tool cache hit, miss, invalidation, and entry counts.",
            input_schema: json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "web_fetch",
            description: "Fetch a URL, convert it into readable text, and answer a prompt about it.",
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
            description: "Federated no-key web search with concurrent public sources, intent routing, deduplication, source receipts, and cited results.",
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
            description: "Update the structured task list with priorities and status tracking.",
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
            description: "Ask the user a clarifying question. Use when ambiguous or need decision.",
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
            description: "Return the current UTC time and local timezone with a bounded RFC3339 timestamp.",
            input_schema: json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "get_context_remaining",
            description: "Return the active conversation's current context utilization and remaining budget in tokens.",
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
            description: "Explicitly unsupported: plugin installation is a control-plane operation performed by an operator. The tool is registered so a model request fails closed with a clear reason instead of being silently ignored.",
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
            name: "tool_search",
            description: "Search for deferred or specialized tools by exact name or keywords.",
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
            description: "Replace, insert, or delete a cell in a Jupyter notebook.",
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
            description: "Wait for a specified duration without holding a shell process.",
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
            description: "Send a message to the user.",
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
            description: "Get or set Cowd settings.",
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
            description: "Enable a worktree-local planning mode override and remember the previous local setting for exit_plan_mode.",
            input_schema: json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
            required_permission: PermissionMode::WorkspaceWrite,
        },
        ToolSpec {
            name: "exit_plan_mode",
            description: "Restore or clear the worktree-local planning mode override created by enter_plan_mode.",
            input_schema: json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
            required_permission: PermissionMode::WorkspaceWrite,
        },
        ToolSpec {
            name: "structured_output",
            description: "Return structured output in the requested format.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": true
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "repl",
            description: "Execute code in a REPL-like subprocess.",
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
            description: "Execute a PowerShell command with optional timeout.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "command": { "type": "string" },
                    "timeout": { "type": "integer", "minimum": 1 },
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
            description: "Ask the user a question and wait for their response.",
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
            description: "Query Language Server Protocol for code intelligence (symbols, references, diagnostics).",
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
            description: "List available resources from connected MCP servers.",
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
            description: "Read a specific resource from an MCP server by URI.",
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
            description: "Authenticate with an MCP server that requires OAuth or credentials.",
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
            description: "Trigger a remote action or webhook endpoint.",
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
            description: "Execute a tool provided by a connected MCP server.",
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
            description: "Test-only tool for verifying permission enforcement behavior.",
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
            description: "Prepare a local image file for multimodal LLM analysis. \
                The tool validates and encodes the image; the runtime then attaches it as a structured vision input on the next model call. \
                Use it when a prompt references an image path that is not already attached as vision input.",
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
            description: "Execute code in a sandboxed interpreter. Use to analyze data programmatically instead of reading raw data into context. \
                Supported: python, javascript, bash, ruby, lua. \
                Only stdout/stderr returned. 30s timeout.",
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
            required_permission: PermissionMode::WorkspaceWrite,
        },
    ]
}
#[cfg(test)]
mod tests {
    use super::{builtin_effect_resolver_spec, mvp_tool_specs, normalize_tool_name};

    #[test]
    fn source_inspection_tools_promote_search_before_bounded_reads() {
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
        assert!(read_file.description.contains("grep_search first"));
        assert!(grep_search.description.contains("Preferred locator"));
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
}
