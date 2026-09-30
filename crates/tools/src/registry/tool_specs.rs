use serde_json::{json, Value};

use crate::permissions::PermissionMode;
use harness_contract::tool::ToolEffectResolverSpec;

/// The capability family a tool belongs to.
///
/// One family is one KIND OF WORK. The structural contract is that every tool
/// declares exactly one family, and a family is not represented by two live
/// tools at the same exposure tier. This is the taxonomy the flat catalog
/// lacked: before it the only groupings were the effect resolver (a permission
/// dimension, not a functional one) and the catalog source
/// (builtin / runtime / plugin).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ToolFamily {
    /// Find files, symbols or content.
    Locate,
    /// Read workspace bytes or Runtime references.
    Read,
    /// Reduce many inputs to a digest outside the model.
    Reduce,
    /// Change files, checkpoints, notebooks or skills.
    Mutate,
    /// Structured planning or execution mode.
    Plan,
    /// Ask the user.
    Ask,
    /// Reach the network.
    Web,
    /// Background jobs and per-session counters.
    Session,
    /// Analyse an image.
    Vision,
    /// Language-server queries.
    Lsp,
    /// Bridge to external MCP tools.
    Mcp,
    /// Discovery, governance and operator-only control.
    Govern,
    /// Drive a bounded program of other tools.
    Orchestrate,
    /// Semantic collaboration actions.
    Collaborate,
}

/// How a tool is exposed to the model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ToolTier {
    /// Always exposed on the first request of a turn: the working set every task
    /// is expected to need.
    Direct,
    /// Reachable through the orchestration vocabulary or on activation.
    Orchestration,
    /// Escape hatch: discovered and activated on demand.
    EscapeHatch,
    /// Registered to fail closed; deliberately outside the model's decision
    /// surface (operator / control-plane only).
    Operator,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolSpec {
    pub name: &'static str,
    /// The capability family. Exactly one.
    pub family: ToolFamily,
    /// The exposure tier.
    pub tier: ToolTier,
    /// The permission family key this tool grants, `"none"` when it grants no
    /// authority beyond reading.
    pub permission_key: &'static str,
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
        "bash" | "powershell" => "builtin.command",
        "edit" => "runtime.action_edit",
        "checkpoint" => "runtime.action_checkpoint",
        "background" => "runtime.action_background",
        // `execute_code` and `repl` always enter the kernel-hardened workspace
        // sandbox with a bounded timeout/output budget. They must not inherit
        // the arbitrary host-shell descriptor used by `bash`, and their
        // registered read-only/no-network effect must stay truthful at
        // execution time.
        "execute_code" => "builtin.sandboxed_command",
        // Effect is the strictest effect among the orchestrated sub-calls; the
        // host computes the escalation and this resolver shapes the descriptor.
        "tool_orchestrate" => "builtin.orchestrate",
        "read_file"
        | "glob_search"
        | "grep_search"
        | "workspace_snapshot"
        | "tool_cache_stats"
        | "checkpoint_list"
        | "checkpoint_diff"
        | "ask"
        | "tool_search"
        | "structured_output"
        | "current_time"
        | "get_context_remaining"
        | "testing_permission" => "builtin.readonly",
        "lsp" => "builtin.readonly_process",
        "vision_analyze" => "builtin.readonly_process",
        "todo_write"
        | "config"
        | "plan_mode" => "builtin.workspace_write",
        "web_fetch" | "web_search" | "remote_trigger"
 => "builtin.network",
        "request_plugin_install" => "builtin.external_unknown",
        "skill" => "runtime.action_skill",
        "sleep" => "builtin.process",
        "mcp" => "runtime.action_mcp",
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
/// The built-in tools the model is meant to call DIRECTLY.
///
/// Derived from the exposure tier rather than hand-listed: `Direct` is the turn
/// working set and `EscapeHatch` is `tool_search` (the discovery mechanism, which
/// must stay reachable or deferred tools can never be activated). `Operator` is
/// deliberately excluded - those tools are registered to fail closed and are not
/// part of the model's decision surface - and `Orchestration` is reachable
/// through the orchestration vocabulary / activation instead of the first
/// request.
#[must_use]
pub fn bootstrap_builtin_tool_ids() -> Vec<&'static str> {
    let mut ids = mvp_tool_specs()
        .into_iter()
        .filter(|spec| matches!(spec.tier, ToolTier::Direct | ToolTier::EscapeHatch))
        .map(|spec| spec.name)
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

pub fn mvp_tool_specs() -> Vec<ToolSpec> {
    vec![
        ToolSpec {
            name: "bash",
            family: ToolFamily::Reduce,
            tier: ToolTier::EscapeHatch,
            permission_key: "shell",
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
                background(action=status) and cancel it with background(action=kill). Do not run \
                interactive commands.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "command": { "type": "string" },
                    "shell": { "type": "string", "enum": ["auto", "bash", "powershell"], "description": "Host shell backend; `auto` picks by platform, `powershell` runs the Windows PowerShell backend." },
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
                        "description": "Run detached and return a job id instead of waiting; query it with background(action=status) and cancel it with background(action=kill)."
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
            name: "background",
            family: ToolFamily::Session,
            tier: ToolTier::Orchestration,
            permission_key: "background",
            description: "Manage this session's background jobs through ONE entry point. action=status (jobId, tail?): running/finished state, exit code, elapsed time, byte counts, persistedOutputPath and a head+tail preview, where tail=N returns only the last N bytes. action=kill (jobId): cancel the job by killing its process group; terminal for that job. Boundary: it only reaches jobs started with run_in_background in this session, status never returns the full output - re-read the persisted range with read_file - and kill is terminal, so check status first if you still need the output.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": ["status", "kill"] },
                    "jobId": { "type": "string" },
                    "tail": { "type": "integer", "minimum": 1, "maximum": 65536 }
                },
                "required": ["action", "jobId"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::WorkspaceWrite,
        },
        ToolSpec {
            name: "read_file",
            family: ToolFamily::Read,
            tier: ToolTier::Direct,
            permission_key: "read",
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
            name: "checkpoint",
            family: ToolFamily::Mutate,
            tier: ToolTier::Orchestration,
            permission_key: "checkpoint",
            description: "Manage local workspace checkpoints through ONE entry point. action=create (label?, paths?): snapshot the workspace before risky edits. action=list: list local checkpoints with ids and scope. action=diff (id): compare the workspace against a checkpoint. action=restore (id): overwrite files from a checkpoint - diff first. Boundary: checkpoints cover workspace content only, never session state, and are local to this session and host.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": ["create", "list", "diff", "restore"] },
                    "id": { "type": "string", "description": "Checkpoint id for diff/restore." },
                    "label": { "type": "string" },
                    "paths": { "type": "array", "items": { "type": "string" }, "description": "Optional workspace-relative paths for a bounded checkpoint." }
                },
                "required": ["action"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::WorkspaceWrite,
        },
        ToolSpec {
            name: "edit",
            family: ToolFamily::Mutate,
            tier: ToolTier::Orchestration,
            permission_key: "edit",
            description: "The ONE workspace mutation primitive. action=write (path, content): create or fully replace a file. action=replace (path, old_string, new_string, replace_all?): replace an exact string in one file; the old string must match exactly once unless replace_all is set. action=preview (edits): dry-run several replacements, reporting resulting content, counts and hash changes without writing - always validate a multi-file edit this way first. action=patch (edits, expected_hashes?): apply a multi-file plan after expected-hash verification, in declaration order under the per-call scope lock. action=notebook (notebook_path, ...): replace, insert or delete a Jupyter cell source without executing it. Boundary: write/preview/patch/replace touch the workspace and need write authorization while preview is read-only; patch refuses the whole transaction on a stale hash, and notebook edits cell source only, never executing it.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": ["write", "replace", "preview", "patch", "notebook"] },
                    "path": { "type": "string" },
                    "content": { "type": "string" },
                    "old_string": { "type": "string" },
                    "new_string": { "type": "string" },
                    "replace_all": { "type": "boolean" },
                    "edits": { "type": "array", "items": { "type": "object", "properties": { "path": { "type": "string" }, "old_string": { "type": "string" }, "new_string": { "type": "string" }, "replace_all": { "type": "boolean" } }, "required": ["path", "old_string", "new_string"], "additionalProperties": false } },
                    "expected_hashes": { "type": "object", "additionalProperties": { "type": "string" } },
                    "notebook_path": { "type": "string" },
                    "cell_id": { "type": "string" },
                    "new_source": { "type": "string" },
                    "cell_type": { "type": "string", "enum": ["code", "markdown"] },
                    "edit_mode": { "type": "string", "enum": ["replace", "insert", "delete"] }
                },
                "required": ["action"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::WorkspaceWrite,
        },
        ToolSpec {
            name: "glob_search",
            family: ToolFamily::Locate,
            tier: ToolTier::Direct,
            permission_key: "glob",
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
            name: "grep_search",
            family: ToolFamily::Locate,
            tier: ToolTier::Direct,
            permission_key: "grep",
            description: "Locate symbols, text or logic inside large files; the cheapest way to locate and then read only the range you need. Three shapes, one tool: (a) a single `pattern` regex returning matching lines with optional context; (b) `searches:[...]` runs several independent searches in ONE ordered batch, each taking the single-search field set; (c) `pattern` + `language` sweeps only that language's file extensions. Boundary: scans are bounded by depth, entries, duration and file size, and this is NOT a structural/AST matcher - the pattern is a plain regex applied per line, so it cannot express a function named X; narrow the candidate set with glob first, and read the hit's range with read_file(offset=nextOffset) instead of re-scanning.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "pattern": { "type": "string" },
                    "language": { "type": "string", "description": "Restrict the sweep to one language's file extensions." },
                    "case_sensitive": { "type": "boolean" },
                    "max_files": { "type": "integer", "minimum": 1, "maximum": 2000 },
                    "max_matches": { "type": "integer", "minimum": 1, "maximum": 500 },
                    "searches": { "type": "array", "maxItems": 200, "items": { "type": "object", "properties": { "pattern": { "type": "string" }, "path": { "type": "string" }, "glob": { "type": "string" }, "output_mode": { "type": "string", "enum": ["content", "files_with_matches", "count"] }, "context": { "type": "integer", "minimum": 0 }, "-n": { "type": "boolean" }, "-i": { "type": "boolean" }, "head_limit": { "type": "integer", "minimum": 1 } }, "required": ["pattern"] }, "description": "Several independent searches in ONE ordered batch." },
                    "max_concurrency": { "type": "integer", "minimum": 1, "maximum": 42 },

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
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "workspace_snapshot",
            family: ToolFamily::Locate,
            tier: ToolTier::Direct,
            permission_key: "read",
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
            name: "tool_orchestrate",
            family: ToolFamily::Orchestrate,
            tier: ToolTier::Direct,
            permission_key: "batch",
            description: "Run a bounded PROGRAM of many tool calls in ONE provider round: one instruction reaches its result without a round trip per step. This is the shortest path whenever the work is more than a single step - locate, read, aggregate and report in one call instead of one call per round. Bounds: <=50 steps, <=400 sub-calls in total, map over <=200 items, repeat <=25 rounds. Sub-calls run in declaration order and share values through `$<step_id>` references inside input objects; write a literal $ as $$. Step shapes, exactly as the input schema accepts them: {op:call,id,tool,input} one sub-call. {op:batch,id,calls:[{tool,input,as?}]} parallel fan-out (the same engine as read_many). {op:map,id,over:$list,tool,input,field?} runs `tool` once per item of a prior list/collect result; `field` extracts that field from each sub-call's RESULT before it is collected (it does not pre-select from the input item). {op:filter,id,over:$list,field,contains?,equals?} keeps the items whose `field` contains or equals a value. {op:collect,id,over:$list,agg,field?,separator?} aggregates a list, where agg is count|first|last|join|sum (join takes `separator`, default a single space). {op:compute,id,language,code,input?} runs code in the kernel-hardened read-only network-off sandbox over the data the program already holds and returns only stdout/stderr - use it to count, aggregate or compare across many files instead of moving their bodies into context. {op:when,id,if:{ref:$id,present?,equals?,contains?,non_empty?},tool,input?} runs one sub-call only when the condition holds. {op:repeat,id,max,while:{ref:$id,...},tool,input?} re-runs one sub-call while the condition holds, at most max times. Vocabulary: any catalog tool may be dispatched - read_file/glob_search/grep_search/workspace_snapshot to locate and read (batch a known file set with a `batch` step of read_file calls, or `map` over a list); execute_code to reduce; write_file/edit_file/apply_patch_transaction to mutate, which needs write authorization; todo_write/checkpoint_*/skill_*/lsp/web_search/web_fetch/mcp to govern and integrate. Agentic/collaboration actions (team_create, agent_invite, task_publish, task_claim, task_submit, task_review, message_publish, artifact_commit, state_inspect, objective_*, ...) are NOT dispatchable here - they are their own provider tools and must be called directly. tool_orchestrate itself may never be nested. Boundaries: every sub-call is authorized independently against the session tool allow-list, so an unauthorized tool fails closed naming that tool - the program never grants extra permission, and a write-requiring program is refused in a read-only session. Output is one JSON object: the `return` expression's value plus a per-step trace (op, tool, status, durationMs, error). Use `return` to select only the digest you need, because the whole result is subject to the tool-result budget.",
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
            family: ToolFamily::Session,
            tier: ToolTier::Orchestration,
            permission_key: "diagnostics",
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
            family: ToolFamily::Web,
            tier: ToolTier::Direct,
            permission_key: "webfetch",
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
            family: ToolFamily::Web,
            tier: ToolTier::Direct,
            permission_key: "websearch",
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
            family: ToolFamily::Plan,
            tier: ToolTier::Direct,
            permission_key: "todowrite",
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
            name: "ask",
            family: ToolFamily::Ask,
            tier: ToolTier::Direct,
            permission_key: "ask",
            description: "Talk to the user through ONE entry point. With `question` (+ optional `options`) it asks and BLOCKS the turn until they answer - use it only when you cannot proceed without a reply. With `message` (+ optional `attachments`, `status`) it sends an update or decision and does NOT wait. Boundary: `question` and `message` are mutually exclusive; never use a blocking question for progress that needs no answer.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "question": { "type": "string", "description": "Ask and block until answered. Mutually exclusive with message." },
                    "options": { "type": "array", "items": { "type": "string" }, "description": "Choices for question." },
                    "message": { "type": "string", "description": "Send without waiting. Mutually exclusive with question." },
                    "attachments": { "type": "array", "items": { "type": "string" } },
                    "status": { "type": "string", "enum": ["normal", "proactive"] }
                },
                "additionalProperties": false
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "current_time",
            family: ToolFamily::Session,
            tier: ToolTier::Orchestration,
            permission_key: "time",
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
            family: ToolFamily::Session,
            tier: ToolTier::Direct,
            permission_key: "context",
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
            family: ToolFamily::Govern,
            tier: ToolTier::Operator,
            permission_key: "plugin",
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
            name: "tool_search",
            family: ToolFamily::Govern,
            tier: ToolTier::EscapeHatch,
            permission_key: "discovery",
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
            name: "sleep",
            family: ToolFamily::Session,
            tier: ToolTier::Orchestration,
            permission_key: "process",
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
            name: "config",
            family: ToolFamily::Govern,
            tier: ToolTier::Operator,
            permission_key: "config",
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
            name: "plan_mode",
            family: ToolFamily::Plan,
            tier: ToolTier::Orchestration,
            permission_key: "plan_mode",
            description: "Toggle the worktree-local planning-mode override. action=enter enables it and remembers the previous local setting; action=exit restores or clears it. Boundary: it only affects this worktree, and exit requires a matching enter in this worktree - with none it fails closed rather than guessing the previous state.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": ["enter", "exit"] }
                },
                "required": ["action"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::WorkspaceWrite,
        },
        ToolSpec {
            name: "structured_output",
            family: ToolFamily::Govern,
            tier: ToolTier::Orchestration,
            permission_key: "none",
            description: "Return the final answer in the requested structured shape. Boundary: it is a presentation contract, not evidence - committed tool receipts still carry the facts.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": true
            }),
            required_permission: PermissionMode::ReadOnly,
        },
        ToolSpec {
            name: "skill",
            family: ToolFamily::Govern,
            tier: ToolTier::Orchestration,
            permission_key: "skill",
            description: "Manage local or GitHub skills through ONE entry point. action=install_plan (source): acquire as inert content, inspect every bounded file, return the canonical SHA-256 tree digest plus blockers/warnings; nothing is activated. action=install_commit (source, expected_digest, allow_warnings?): re-acquire, require the exact digest, enforce blockers, atomically activate an immutable revision and emit a provenance receipt. action=status (skill_id): read the active pointer, revision history and receipts. action=rollback (skill_id, revision): atomically reactivate an existing revision after digest verification. action=deactivate (skill_id): remove the active pointer while retaining immutable revisions. Boundary: it never executes packaged code and never deletes revisions, and a digest mismatch refuses.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": ["install_plan", "install_commit", "status", "rollback", "deactivate"] },
                    "source": { "type": "string", "minLength": 1 },
                    "expected_digest": { "type": "string", "pattern": "^sha256:[0-9a-f]{64}$" },
                    "allow_warnings": { "type": "boolean", "default": false },
                    "skill_id": { "type": "string", "minLength": 1 },
                    "revision": { "type": "string", "pattern": "^sha256:[0-9a-f]{64}$" }
                },
                "required": ["action"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::DangerFullAccess,
        },
        ToolSpec {
            name: "lsp",
            family: ToolFamily::Lsp,
            tier: ToolTier::Orchestration,
            permission_key: "lsp",
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
            name: "remote_trigger",
            family: ToolFamily::Web,
            tier: ToolTier::Operator,
            permission_key: "webfetch",
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
            family: ToolFamily::Mcp,
            tier: ToolTier::Orchestration,
            permission_key: "mcp",
            description: "The ONE bridge to externally connected MCP servers. action=call (server, tool, arguments?): execute a tool the server exposes. action=list (server?): list the resources the server publishes. action=read (server?, uri): read one published resource. action=auth (server): authenticate when a server reports an auth failure and the credential needs no human input. Prefer local tools for anything the catalog already covers; the server must be configured and reachable, and arguments pass through unvalidated. Boundary: list/read are read-only; call and auth need full access, and auth never stores or prints a secret.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": ["call", "list", "read", "auth"] },
                    "server": { "type": "string" },
                    "tool": { "type": "string" },
                    "arguments": { "type": "object" },
                    "uri": { "type": "string" }
                },
                "required": ["action"],
                "additionalProperties": false
            }),
            required_permission: PermissionMode::DangerFullAccess,
        },
        ToolSpec {
            name: "testing_permission",
            family: ToolFamily::Govern,
            tier: ToolTier::Operator,
            permission_key: "testing",
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
            family: ToolFamily::Vision,
            tier: ToolTier::Orchestration,
            permission_key: "vision",
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
            family: ToolFamily::Reduce,
            tier: ToolTier::Direct,
            permission_key: "code",
            description: "Deterministic reduction OUTSIDE the model. Runs python/javascript/bash/ruby/lua in a kernel-hardened, read-only, network-off sandbox and returns ONLY stdout/stderr (30s cap, nothing persisted). This is the tool for work that would otherwise mean pulling many files or many lines into context: count, aggregate, filter, compare, compute. The workspace is mounted read-only with the working directory inside it, so the code CAN read authorised workspace files; it cannot write and cannot reach the network. Boundary: it cannot write files or touch the network - feed it data from read_file/read_many/grep_search, or dispatch it as tool_orchestrate's compute step where the program already holds the data.",
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
                    },
                    "timeout_ms": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Optional wall-clock timeout in milliseconds; defaults to the sandbox budget."
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
    fn skill_supply_chain_separates_review_from_mutation() {
        let specs = mvp_tool_specs();
        let skill = specs
            .iter()
            .find(|spec| spec.name == "skill")
            .expect("skill tool");
        // The strictest action is the static floor, which is what the
        // orchestration escalation path uses (it cannot see sub-call input).
        // The action-aware resolver lowers it to ReadOnly for query actions.
        assert_eq!(skill.required_permission, PermissionMode::DangerFullAccess);
        assert_eq!(
            builtin_effect_resolver_spec(skill.name).resolver_id,
            "runtime.action_skill"
        );
    }

    #[test]
    fn shell_timeout_contract_uses_explicit_milliseconds_only() {
        for name in ["bash"] {
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
    const ALIASES: &[&str] = &[];

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
    // The regex-only reality of the old `ast_grep_search` is now stated on the
    // one `grep_search` tool, so the model cannot believe a structural matcher
    // exists when it does not.
    let grep = by_name("grep_search");
    assert!(
        !grep.description.to_lowercase().contains("code construct"),
        "grep_search is a per-line regex, not a structural matcher: {}",
        grep.description
    );
    assert!(
        grep.description.contains("NOT a structural/AST matcher"),
        "grep_search must say what it is not: {}",
        grep.description
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

#[cfg(test)]
mod family_taxonomy_tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    /// Documented aliases defer to a canonical tool, so they never occupy a
    /// permission slot of their own.
    const ALIASES: &[&str] = &["edit_many_preview", "patch_plan"];

    /// The taxonomy the flat catalog lacked: every tool declares exactly one
    /// capability family and exposure tier and a non-empty permission key, and
    /// the family assignment is total.
    #[test]
    fn every_tool_declares_a_family_tier_and_permission_key() {
        let specs = mvp_tool_specs();
        assert_eq!(specs.len(), 29, "the catalog size is the contract");
        for spec in &specs {
            assert!(
                !spec.permission_key.is_empty(),
                "`{}` has no permission key",
                spec.name
            );
        }
        let families = specs.iter().map(|spec| spec.family).collect::<BTreeSet<_>>();
        assert!(
            families.len() >= 10,
            "the taxonomy must actually partition the catalog"
        );
        // The Direct tier is the turn working set: an ordinary turn needs these
        // on its very first request, so they cannot be deferred.
        let direct = specs
            .iter()
            .filter(|spec| spec.tier == ToolTier::Direct)
            .map(|spec| spec.name)
            .collect::<BTreeSet<_>>();
        for required in [
            "read_file",
            "grep_search",
            "glob_search",
            "tool_orchestrate",
            "execute_code",
            "todo_write",
        ] {
            assert!(
                direct.contains(required),
                "`{required}` must be Direct: an ordinary turn needs it on the first request"
            );
        }
        assert!(
            specs
                .iter()
                .filter(|spec| spec.tier == ToolTier::Operator)
                .all(|spec| spec.permission_key != "discovery"),
            "operator-tier tools are never the discovery surface"
        );
    }

    /// A permission slot is `(family, permission_key, tier)`. Two live *canonical*
    /// tools in one slot is exactly the "many tools for one job" ambiguity this
    /// taxonomy exists to remove.
    ///
    /// The catalog still carries legacy collisions, so this test freezes them:
    /// no NEW one may appear, and merging a family removes its ledger entry. The
    /// ledger may only shrink.
    #[test]
    fn canonical_tools_do_not_share_a_permission_slot_beyond_the_legacy_ledger() {
        const LEGACY: &[(&str, &str, &str, usize)] = &[
        ];
        let mut slots: BTreeMap<(&str, &str, &str), usize> = BTreeMap::new();
        for spec in mvp_tool_specs() {
            if ALIASES.contains(&spec.name) {
                continue;
            }
            let slot = (
                family_name(spec.family),
                spec.permission_key,
                tier_name(spec.tier),
            );
            *slots.entry(slot).or_default() += 1;
        }
        let actual = slots
            .into_iter()
            .filter(|(_, count)| *count >= 2)
            .collect::<BTreeMap<_, _>>();
        let expected = LEGACY
            .iter()
            .map(|(family, key, tier, count)| ((*family, *key, *tier), *count))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            actual, expected,
            "a permission slot may only hold the legacy collisions already recorded: \
             merging removes entries, nothing may add one"
        );
    }

    /// The first-request working set is derived from the tiers, not hand-listed.
    /// Direct and EscapeHatch are exposed; Orchestration and Operator are not.
    #[test]
    fn the_bootstrap_set_is_derived_from_the_tiers() {
        let ids = bootstrap_builtin_tool_ids();
        for spec in mvp_tool_specs() {
            let exposed = ids.contains(&spec.name);
            match spec.tier {
                ToolTier::Direct | ToolTier::EscapeHatch => {
                    assert!(exposed, "`{}` is Direct/EscapeHatch and must be bootstrap", spec.name)
                }
                ToolTier::Orchestration | ToolTier::Operator => assert!(
                    !exposed,
                    "`{}` is {} and must not be bootstrap",
                    spec.name,
                    tier_name(spec.tier)
                ),
            }
        }
        assert!(ids.contains(&"tool_search"), "discovery must stay reachable");
        assert!(
            !ids.contains(&"testing_permission"),
            "operator-tier tools never enter the model surface"
        );
    }

    fn family_name(family: ToolFamily) -> &'static str {
        match family {
            ToolFamily::Locate => "Locate",
            ToolFamily::Read => "Read",
            ToolFamily::Reduce => "Reduce",
            ToolFamily::Mutate => "Mutate",
            ToolFamily::Plan => "Plan",
            ToolFamily::Ask => "Ask",
            ToolFamily::Web => "Web",
            ToolFamily::Session => "Session",
            ToolFamily::Vision => "Vision",
            ToolFamily::Lsp => "Lsp",
            ToolFamily::Mcp => "Mcp",
            ToolFamily::Govern => "Govern",
            ToolFamily::Orchestrate => "Orchestrate",
            ToolFamily::Collaborate => "Collaborate",
        }
    }

    fn tier_name(tier: ToolTier) -> &'static str {
        match tier {
            ToolTier::Direct => "Direct",
            ToolTier::Orchestration => "Orchestration",
            ToolTier::EscapeHatch => "EscapeHatch",
            ToolTier::Operator => "Operator",
        }
    }
}
