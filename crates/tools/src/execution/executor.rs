use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use base64::Engine;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::bash::{BashCommandInput, BashCommandOutput};
use crate::checkpoint::{
    checkpoint_create_in, checkpoint_diff_in, checkpoint_list_in, checkpoint_restore_in,
    CheckpointCreateInput, CheckpointDiffInput, CheckpointRestoreInput,
};
use crate::file_ops::{
    edit_file, glob_search, grep_search, read_file, write_file, GrepSearchInput,
};
use crate::lane_events::{LaneEvent, LaneEventName, LaneEventStatus, LaneFailureClass};
use crate::lane_policy::{iso8601_now, LaneContext};
use crate::mutation_plan::{
    apply_mutations, preview_mutations, MutationApplyInput, MutationPreviewInput,
};
use crate::path_policy::WorkspacePathPolicy;
use crate::prepared::{
    prepare_readonly_invocations, PreparedReadonlyLeaf, PreparedReadonlyLeafInvocation,
    PreparedToolCall, ToolExecutionContext,
};
use crate::search::{execute_web_search, WebSearchInput};
use crate::stale_branch::{check_freshness, BranchFreshness};
use crate::ToolHostLease;

#[path = "dispatch.rs"]
mod dispatch;
pub(crate) use dispatch::execute_with_lease;
#[path = "builtin.rs"]
mod builtin;
use builtin::*;

#[cfg(test)]
pub fn execute_tool_for_test(name: &str, input: &Value) -> Result<String, String> {
    TEST_TOOL_HOST.with(|host| execute_with_lease(&host.pin_snapshot(), name, input))
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;

#[cfg(test)]
thread_local! {
    static TEST_TOOL_HOST: crate::ToolHost = crate::ToolHost::builtin(
        "tools-test",
        std::env::current_dir().unwrap_or_default(),
    );
}

fn from_value<T: for<'de> Deserialize<'de>>(input: &Value) -> Result<T, String> {
    serde_json::from_value(input.clone()).map_err(|error| error.to_string())
}

fn run_bash(lease: &ToolHostLease, input: BashCommandInput) -> Result<String, String> {
    if let Some(output) = workspace_test_branch_preflight(&input.command, None) {
        return serde_json::to_string_pretty(&output).map_err(|error| error.to_string());
    }
    serde_json::to_string_pretty(
        &crate::bash::execute_bash_in_workspace(input, lease.workspace_root())
            .map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
}

#[derive(Debug, Deserialize)]
struct AstGrepSearchInput {
    pattern: String,
    language: String,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    case_sensitive: bool,
    #[serde(default = "default_ast_max_files")]
    max_files: usize,
    #[serde(default = "default_ast_max_matches")]
    max_matches: usize,
}

fn default_ast_max_files() -> usize {
    200
}

fn default_ast_max_matches() -> usize {
    50
}

fn ast_language_extensions(language: &str) -> Vec<&'static str> {
    match language.trim().to_ascii_lowercase().as_str() {
        "rust" => vec!["rs"],
        "python" | "py" => vec!["py", "pyi"],
        "typescript" | "ts" => vec!["ts", "tsx", "mts", "cts"],
        "javascript" | "js" => vec!["js", "jsx", "mjs", "cjs"],
        "go" => vec!["go"],
        "java" => vec!["java"],
        "c" => vec!["c", "h"],
        "cpp" | "c++" => vec!["cpp", "cc", "cxx", "hpp", "hh", "hxx"],
        "csharp" | "c#" => vec!["cs"],
        "ruby" | "rb" => vec!["rb"],
        "php" => vec!["php"],
        "shell" | "bash" | "sh" => vec!["sh", "bash"],
        "sql" => vec!["sql"],
        "toml" => vec!["toml"],
        "yaml" | "yml" => vec!["yaml", "yml"],
        "json" => vec!["json"],
        "markdown" | "md" => vec!["md", "markdown"],
        _ => vec![
            "rs", "py", "ts", "tsx", "js", "jsx", "go", "java", "c", "h", "cpp", "hpp", "cs", "rb",
            "php", "sh", "sql",
        ],
    }
}

fn run_ast_grep_search(lease: &ToolHostLease, input: AstGrepSearchInput) -> Result<String, String> {
    if input.pattern.trim().is_empty() {
        return Err("ast_grep_search requires a non-empty pattern".to_string());
    }
    let workspace = lease
        .workspace_root()
        .canonicalize()
        .map_err(|error| error.to_string())?;
    let mut root = workspace.clone();
    if let Some(sub) = input
        .path
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        root.push(sub);
    }
    let root = root
        .canonicalize()
        .map_err(|error| format!("ast_grep_search path invalid: {error}"))?;
    if !root.starts_with(&workspace) {
        return Err("ast_grep_search path escapes the workspace".to_string());
    }
    let regex = if input.case_sensitive {
        regex::Regex::new(&input.pattern).map_err(|error| format!("invalid pattern: {error}"))?
    } else {
        regex::RegexBuilder::new(&input.pattern)
            .case_insensitive(true)
            .build()
            .map_err(|error| format!("invalid pattern: {error}"))?
    };
    let extensions = ast_language_extensions(&input.language);
    let max_files = input.max_files.max(1).min(2_000);
    let max_matches = input.max_matches.max(1).min(500);
    let mut matches = Vec::new();
    let mut files_scanned = 0usize;
    for entry in walkdir::WalkDir::new(&root)
        .into_iter()
        .filter_map(|entry| entry.ok())
    {
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        let rel = path.strip_prefix(&root).unwrap_or(path);
        let first = rel
            .components()
            .next()
            .and_then(|component| component.as_os_str().to_str())
            .unwrap_or("");
        if matches!(first, "target" | ".git" | "node_modules" | ".cowd") {
            continue;
        }
        if !extensions.iter().any(|ext| {
            path.extension()
                .map_or(false, |extension| extension == *ext)
        }) {
            continue;
        }
        files_scanned += 1;
        if files_scanned > max_files {
            break;
        }
        let content = std::fs::read(path)
            .map_err(|error| format!("ast_grep_search read {}: {error}", path.display()))?;
        if content.len() > 512 * 1024 {
            continue;
        }
        let text = String::from_utf8_lossy(&content);
        for (line_index, line) in text.lines().enumerate() {
            if let Some(found) = regex.find(line) {
                matches.push(serde_json::json!({
                    "path": rel.display().to_string(),
                    "line": line_index + 1,
                    "column": found.start() + 1,
                    "text": line.trim().chars().take(300).collect::<String>(),
                }));
                if matches.len() >= max_matches {
                    break;
                }
            }
        }
        if matches.len() >= max_matches {
            break;
        }
    }
    serde_json::to_string_pretty(&serde_json::json!({
        "kind": "ast_grep_search",
        "pattern": input.pattern,
        "language": input.language,
        "files_scanned": files_scanned,
        "match_count": matches.len(),
        "matches": matches,
    }))
    .map_err(|error| error.to_string())
}

fn workspace_test_branch_preflight(
    command: &str,
    mut lane_ctx: Option<&mut LaneContext>,
) -> Option<BashCommandOutput> {
    if !is_workspace_test_command(command) {
        return None;
    }

    let branch = git_stdout(&["branch", "--show-current"])?;
    let main_ref = resolve_main_ref(&branch)?;
    let freshness = check_freshness(&branch, &main_ref);
    // Also populate lane context for policy evaluation
    if let Some(ref mut ctx) = lane_ctx {
        ctx.stale_branch = Some(freshness.clone());
        ctx.branch_freshness = match &freshness {
            BranchFreshness::Stale { .. } | BranchFreshness::Diverged { .. } => {
                Duration::from_secs(999999)
            }
            BranchFreshness::Fresh => Duration::from_secs(0),
        };
    }
    match freshness {
        BranchFreshness::Fresh => None,
        BranchFreshness::Stale {
            commits_behind,
            missing_fixes,
        } => Some(branch_divergence_output(
            command,
            &branch,
            &main_ref,
            commits_behind,
            None,
            &missing_fixes,
        )),
        BranchFreshness::Diverged {
            ahead,
            behind,
            missing_fixes,
        } => Some(branch_divergence_output(
            command,
            &branch,
            &main_ref,
            behind,
            Some(ahead),
            &missing_fixes,
        )),
    }
}

fn is_workspace_test_command(command: &str) -> bool {
    let normalized = normalize_shell_command(command);
    [
        "cargo test --workspace",
        "cargo test --all",
        "cargo nextest run --workspace",
        "cargo nextest run --all",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
}

fn normalize_shell_command(command: &str) -> String {
    command
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn resolve_main_ref(branch: &str) -> Option<String> {
    let has_local_main = git_ref_exists("main");
    let has_remote_main = git_ref_exists("origin/main");

    if branch == "main" && has_remote_main {
        Some("origin/main".to_string())
    } else if has_local_main {
        Some("main".to_string())
    } else if has_remote_main {
        Some("origin/main".to_string())
    } else {
        None
    }
}

fn git_ref_exists(reference: &str) -> bool {
    Command::new("git")
        .args(["rev-parse", "--verify", "--quiet", reference])
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn git_stdout(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!stdout.is_empty()).then_some(stdout)
}

fn branch_divergence_output(
    command: &str,
    branch: &str,
    main_ref: &str,
    commits_behind: usize,
    commits_ahead: Option<usize>,
    missing_fixes: &[String],
) -> BashCommandOutput {
    let relation = commits_ahead.map_or_else(
        || format!("is {commits_behind} commit(s) behind"),
        |ahead| format!("has diverged ({ahead} ahead, {commits_behind} behind)"),
    );
    let missing_summary = if missing_fixes.is_empty() {
        "(none surfaced)".to_string()
    } else {
        missing_fixes.join("; ")
    };
    let stderr = format!(
        "branch divergence detected before workspace tests: `{branch}` {relation} `{main_ref}`. Missing commits: {missing_summary}. Merge or rebase `{main_ref}` before re-running `{command}`."
    );

    let lane_event = LaneEvent::new(
        LaneEventName::BranchStaleAgainstMain,
        LaneEventStatus::Blocked,
        iso8601_now(),
    )
    .with_failure_class(LaneFailureClass::BranchDivergence)
    .with_detail(stderr.clone())
    .with_data(json!({
        "branch": branch,
        "mainRef": main_ref,
        "commitsBehind": commits_behind,
        "commitsAhead": commits_ahead,
        "missingCommits": missing_fixes,
        "blockedCommand": command,
        "recommendedAction": format!("merge or rebase {main_ref} before workspace tests")
    }));

    BashCommandOutput {
        stdout: String::new(),
        stderr: stderr.clone(),
        raw_output_path: None,
        interrupted: false,
        is_image: None,
        background_task_id: None,
        backgrounded_by_user: None,
        assistant_auto_backgrounded: None,
        dangerously_disable_sandbox: None,
        return_code_interpretation: Some("preflight_blocked:branch_divergence".to_string()),
        no_output_expected: Some(false),
        structured_content: serde_json::to_value(lane_event)
            .ok()
            .map(|event| vec![event]),
        persisted_output_path: None,
        persisted_output_size: None,
        sandbox_status: None,
        return_truncated: false,
        progress: Vec::new(),
    }
}

#[allow(clippy::needless_pass_by_value)]
fn run_read_file(lease: &ToolHostLease, input: ReadFileInput) -> Result<String, String> {
    let resolved = lease
        .path_policy()
        .resolve(&input.path)
        .map_err(io_to_string)?;
    let fingerprint = file_fingerprint(&resolved);
    let scope = file_cache_scope(&resolved);
    cached_json_tool(lease, "read_file", &input, &fingerprint, &scope, || {
        read_file(
            lease.path_policy(),
            &input.path,
            input.offset,
            input.limit,
            input.complete,
        )
        .map_err(io_to_string)
    })
}

#[allow(clippy::needless_pass_by_value)]
fn run_read_many(lease: &ToolHostLease, input: ReadManyInput) -> Result<String, String> {
    let results = run_ordered_batch(input.files, input.max_concurrency, |item| {
        let output = run_read_file(lease, item)?;
        serde_json::from_str(&output).or(Ok(Value::String(output)))
    });
    to_pretty_json(batch_output("read_many", results))
}

#[allow(clippy::needless_pass_by_value)]
fn run_write_file(lease: &ToolHostLease, input: WriteFileInput) -> Result<String, String> {
    let scope = file_cache_scope(
        &lease
            .path_policy()
            .resolve(&input.path)
            .map_err(io_to_string)?,
    );
    create_auto_checkpoint(lease, "write_file")?;
    let output = to_pretty_json(
        write_file(lease.path_policy(), &input.path, &input.content).map_err(io_to_string)?,
    );
    if output.is_ok() {
        lease.cache().invalidate_scope(&scope);
    }
    output
}

#[allow(clippy::needless_pass_by_value)]
fn run_edit_file(lease: &ToolHostLease, input: EditFileInput) -> Result<String, String> {
    let scope = file_cache_scope(
        &lease
            .path_policy()
            .resolve(&input.path)
            .map_err(io_to_string)?,
    );
    create_auto_checkpoint(lease, "edit_file")?;
    let output = to_pretty_json(
        edit_file(
            lease.path_policy(),
            &input.path,
            &input.old_string,
            &input.new_string,
            input.replace_all.unwrap_or(false),
        )
        .map_err(io_to_string)?,
    );
    if output.is_ok() {
        lease.cache().invalidate_scope(&scope);
    }
    output
}

#[allow(clippy::needless_pass_by_value)]
fn run_mutation_preview(
    lease: &ToolHostLease,
    input: MutationPreviewInput,
) -> Result<String, String> {
    to_pretty_json(preview_mutations(lease.path_policy(), input).map_err(io_to_string)?)
}

#[allow(clippy::needless_pass_by_value)]
fn run_apply_patch_transaction(
    lease: &ToolHostLease,
    input: MutationApplyInput,
) -> Result<String, String> {
    create_auto_checkpoint(lease, "apply_patch_transaction")?;
    let applied = apply_mutations(lease.path_policy(), input).map_err(io_to_string)?;
    for file in &applied.applied {
        lease.cache().invalidate_scope(&file_cache_scope(
            &lease
                .path_policy()
                .resolve(&file.path)
                .map_err(io_to_string)?,
        ));
    }
    to_pretty_json(applied)
}

#[allow(clippy::needless_pass_by_value)]
fn run_checkpoint_create(
    lease: &ToolHostLease,
    input: CheckpointCreateInput,
) -> Result<String, String> {
    to_pretty_json(checkpoint_create_in(lease.workspace_root(), input).map_err(io_to_string)?)
}

fn create_auto_checkpoint(lease: &ToolHostLease, tool_name: &str) -> Result<(), String> {
    if std::env::var("COWD_AUTO_CHECKPOINT").ok().as_deref() != Some("1") {
        return Ok(());
    }
    checkpoint_create_in(
        lease.workspace_root(),
        CheckpointCreateInput {
            label: Some(format!("auto-before-{tool_name}")),
            paths: Vec::new(),
        },
    )
    .map(|_| ())
    .map_err(io_to_string)
}

fn run_checkpoint_list(lease: &ToolHostLease) -> Result<String, String> {
    to_pretty_json(checkpoint_list_in(lease.workspace_root()).map_err(io_to_string)?)
}

#[allow(clippy::needless_pass_by_value)]
fn run_checkpoint_diff(
    lease: &ToolHostLease,
    input: CheckpointDiffInput,
) -> Result<String, String> {
    to_pretty_json(checkpoint_diff_in(lease.workspace_root(), input).map_err(io_to_string)?)
}

#[allow(clippy::needless_pass_by_value)]
fn run_checkpoint_restore(
    lease: &ToolHostLease,
    input: CheckpointRestoreInput,
) -> Result<String, String> {
    let output =
        to_pretty_json(checkpoint_restore_in(lease.workspace_root(), input).map_err(io_to_string)?);
    if output.is_ok() {
        lease.cache().invalidate_all();
    }
    output
}

#[allow(clippy::needless_pass_by_value)]
fn run_glob_search(lease: &ToolHostLease, input: GlobSearchInputValue) -> Result<String, String> {
    // Empty/missing paths are normalized to the workspace root so models are
    // not blocked on their first discovery call.
    let resolved_path = input
        .path
        .as_deref()
        .filter(|path| !path.trim().is_empty())
        .unwrap_or(".");
    let fingerprint = scope_fingerprint(lease.path_policy(), Some(resolved_path))?;
    let scope = directory_cache_scope(lease.path_policy(), Some(resolved_path))?;
    cached_json_tool(lease, "glob_search", &input, &fingerprint, &scope, || {
        glob_search(lease.path_policy(), &input.pattern, Some(resolved_path)).map_err(io_to_string)
    })
}

#[allow(clippy::needless_pass_by_value)]
fn run_glob_many(lease: &ToolHostLease, input: GlobManyInput) -> Result<String, String> {
    let results = run_ordered_batch(input.patterns, input.max_concurrency, |item| {
        let output = run_glob_search(lease, item)?;
        serde_json::from_str(&output).or(Ok(Value::String(output)))
    });
    to_pretty_json(batch_output("glob_many", results))
}

#[allow(clippy::needless_pass_by_value)]
fn run_grep_search(lease: &ToolHostLease, input: GrepSearchInput) -> Result<String, String> {
    let fingerprint = scope_fingerprint(lease.path_policy(), input.path.as_deref())?;
    let scope = directory_cache_scope(lease.path_policy(), input.path.as_deref())?;
    cached_json_tool(lease, "grep_search", &input, &fingerprint, &scope, || {
        grep_search(lease.path_policy(), &input).map_err(io_to_string)
    })
}

#[allow(clippy::needless_pass_by_value)]
fn run_grep_many(lease: &ToolHostLease, input: GrepManyInput) -> Result<String, String> {
    let results = run_ordered_batch(input.searches, input.max_concurrency, |item| {
        let output = run_grep_search(lease, item)?;
        serde_json::from_str(&output).or(Ok(Value::String(output)))
    });
    to_pretty_json(batch_output("grep_many", results))
}

#[allow(clippy::needless_pass_by_value)]
fn run_workspace_snapshot(
    lease: &ToolHostLease,
    input: WorkspaceSnapshotInput,
) -> Result<String, String> {
    let snapshot_input = input.clone();
    let fingerprint = workspace_snapshot_fingerprint(lease.path_policy(), &input)?;
    cached_json_tool(
        lease,
        "workspace_snapshot",
        &input,
        &fingerprint,
        "workspace:.",
        || workspace_snapshot_value(lease, snapshot_input),
    )
}

fn workspace_snapshot_value(
    lease: &ToolHostLease,
    input: WorkspaceSnapshotInput,
) -> Result<Value, String> {
    let include_git = input.include_git.unwrap_or(true);
    let include_files = input.include_files.unwrap_or(true);
    let max_files = input.max_files.unwrap_or(500).clamp(1, 5000);
    let cwd = lease.path_policy().workspace_root().to_path_buf();

    let git = if include_git {
        Some(json!({
            "branch": git_stdout(&["rev-parse", "--abbrev-ref", "HEAD"]),
            "status": git_stdout(&["status", "--short", "--branch"]),
            "head": git_stdout(&["rev-parse", "--short", "HEAD"])
        }))
    } else {
        None
    };

    let roots = input.roots.unwrap_or_else(|| vec![String::from(".")]);
    let mut resolved_roots = Vec::new();
    let mut scan_complete = include_files;
    let files = if include_files {
        let mut files = Vec::new();
        for root in roots {
            if files.len() >= max_files {
                scan_complete = false;
                break;
            }
            let root_path = lease.path_policy().resolve(&root).map_err(io_to_string)?;
            resolved_roots.push(root_path.to_string_lossy().into_owned());
            scan_complete &=
                collect_snapshot_files(lease.path_policy(), &root_path, max_files, &mut files);
        }
        files.sort();
        files.dedup();
        files.truncate(max_files);
        Some(files)
    } else {
        None
    };

    Ok(json!({
        "type": "workspace_snapshot",
        "cwd": cwd.to_string_lossy(),
        "git": git,
        "files": files,
        "maxFiles": max_files,
        "resolvedRoots": resolved_roots,
        "scanComplete": scan_complete
    }))
}

fn cached_json_tool<T, F, O>(
    lease: &ToolHostLease,
    tool_name: &str,
    input: &T,
    fingerprint: &str,
    scope: &str,
    operation: F,
) -> Result<String, String>
where
    T: Serialize,
    F: FnOnce() -> Result<O, String>,
    O: Serialize,
{
    const UNCACHEABLE_FINGERPRINT: &str = "uncacheable";
    let input_json = serde_json::to_string(input).map_err(|error| error.to_string())?;
    if fingerprint == UNCACHEABLE_FINGERPRINT {
        return to_pretty_json(operation()?);
    }
    let cache_input = format!("{input_json}::fingerprint::{fingerprint}");
    if let Some(cached) = lease.cache().get(
        lease.workspace_id(),
        scope,
        tool_name,
        &cache_input,
        lease.schema_revision(),
    ) {
        return Ok(cached);
    }
    let output = to_pretty_json(operation()?)?;
    lease.cache().put(
        lease.workspace_id(),
        scope,
        tool_name,
        &cache_input,
        lease.schema_revision(),
        &output,
    );
    Ok(output)
}

fn file_cache_scope(path: &Path) -> String {
    format!("file:{}", path.to_string_lossy())
}

fn directory_cache_scope(
    policy: &WorkspacePathPolicy,
    path: Option<&str>,
) -> Result<String, String> {
    match path {
        Some(path) => Ok(format!(
            "directory:{}",
            policy
                .resolve(path)
                .map_err(io_to_string)?
                .to_string_lossy()
        )),
        None => Ok("workspace:.".to_string()),
    }
}

fn file_fingerprint(resolved: &Path) -> String {
    let content_hash = match std::fs::File::open(resolved) {
        Ok(mut file) => {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            let mut buffer = [0_u8; 8192];
            loop {
                match file.read(&mut buffer) {
                    Ok(0) => break format!("{:016x}", hasher.finish()),
                    Ok(read) => buffer[..read].hash(&mut hasher),
                    Err(_) => break String::from("unreadable"),
                }
            }
        }
        Err(_) => String::from("missing"),
    };
    let Ok(metadata) = std::fs::metadata(resolved) else {
        return format!("missing:{}", resolved.to_string_lossy());
    };
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!(
        "file:{}:{}:{}",
        resolved.to_string_lossy(),
        metadata.len(),
        modified
    ) + &format!(":{content_hash}")
}

fn scope_fingerprint(policy: &WorkspacePathPolicy, path: Option<&str>) -> Result<String, String> {
    let root = match path {
        Some(path) => policy.resolve(path).map_err(io_to_string)?,
        None => policy.workspace_root().to_path_buf(),
    };
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    root.to_string_lossy().hash(&mut hasher);
    let complete = hash_path_scope(&root, &mut hasher, &mut 0usize);
    if !complete {
        return Ok(String::from("uncacheable"));
    }
    Ok(format!("scope:{:016x}", hasher.finish()))
}

fn workspace_snapshot_fingerprint(
    policy: &WorkspacePathPolicy,
    input: &WorkspaceSnapshotInput,
) -> Result<String, String> {
    let mut parts = Vec::new();
    if input.include_git.unwrap_or(true) {
        parts.push(git_stdout(&["rev-parse", "HEAD"]).unwrap_or_default());
        parts.push(git_stdout(&["status", "--short"]).unwrap_or_default());
    }
    if input.include_files.unwrap_or(true) {
        let roots = input
            .roots
            .clone()
            .unwrap_or_else(|| vec![String::from(".")]);
        for root in roots {
            parts.push(scope_fingerprint(policy, Some(&root))?);
        }
    }
    Ok(parts.join("\n"))
}

fn hash_path_scope(
    root: &Path,
    hasher: &mut std::collections::hash_map::DefaultHasher,
    seen: &mut usize,
) -> bool {
    const MAX_FINGERPRINT_FILES: usize = 2048;
    if *seen >= MAX_FINGERPRINT_FILES || should_skip_cache_fingerprint(root) {
        return *seen < MAX_FINGERPRINT_FILES;
    }
    let Ok(metadata) = std::fs::metadata(root) else {
        "missing".hash(hasher);
        root.to_string_lossy().hash(hasher);
        return true;
    };
    root.to_string_lossy().hash(hasher);
    metadata.len().hash(hasher);
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
        .unwrap_or_default()
        .hash(hasher);
    if metadata.is_file() {
        if let Ok(mut file) = std::fs::File::open(root) {
            let mut buffer = [0_u8; 8192];
            loop {
                match file.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(read) => buffer[..read].hash(hasher),
                    Err(_) => {
                        "unreadable".hash(hasher);
                        break;
                    }
                }
            }
        }
        *seen += 1;
        return *seen <= MAX_FINGERPRINT_FILES;
    }
    let Ok(entries) = std::fs::read_dir(root) else {
        return true;
    };
    let mut paths = entries
        .flatten()
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    paths.sort();
    for path in paths {
        if !hash_path_scope(&path, hasher, seen) {
            return false;
        }
        if *seen >= MAX_FINGERPRINT_FILES {
            return false;
        }
    }
    true
}

fn should_skip_cache_fingerprint(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            matches!(
                name,
                ".git" | ".cowd" | "target" | "node_modules" | "dist" | "build" | ".cache"
            )
        })
}

#[allow(clippy::needless_pass_by_value)]
fn run_tool_batch_readonly(
    lease: &ToolHostLease,
    input: ToolBatchReadonlyInput,
) -> Result<String, String> {
    for call in &input.calls {
        if !is_allowed_readonly_batch_tool(&call.name) {
            return Err(format!(
                "tool_batch_readonly only accepts approved read-only tools; `{}` is not allowed",
                call.name
            ));
        }
    }

    let prepared_calls = input
        .calls
        .iter()
        .map(|call| PreparedToolCall {
            name: call.name.clone(),
            input: call.input.clone(),
        })
        .collect::<Vec<_>>();
    let context =
        ToolExecutionContext::for_workspace(lease.workspace_root(), "tool_batch_readonly");
    if calls_support_prepared_readonly(&input.calls) {
        let prepared = prepare_readonly_invocations(&context, &prepared_calls)
            .map_err(|error| error.message)?;
        let results = run_ordered_batch(prepared, input.max_concurrency, |prepared| {
            execute_prepared_readonly_leaf(lease, prepared)
        });
        return to_pretty_json(batch_output_with_mode(
            "tool_batch_readonly",
            "prepared_readonly",
            results,
        ));
    }

    let results = run_ordered_batch(input.calls, input.max_concurrency, |call| {
        let output = execute_with_lease(lease, &call.name, &call.input)?;
        Ok(serde_json::from_str(&output).unwrap_or(Value::String(output)))
    });
    to_pretty_json(batch_output_with_mode(
        "tool_batch_readonly",
        "compat_recursive",
        results,
    ))
}

fn calls_support_prepared_readonly(calls: &[ToolBatchReadonlyCallInput]) -> bool {
    calls
        .iter()
        .all(|call| is_prepared_readonly_tool(&call.name))
}

fn execute_prepared_readonly_leaf(
    lease: &ToolHostLease,
    prepared: PreparedReadonlyLeafInvocation,
) -> Result<Value, String> {
    let output = match prepared.leaf {
        PreparedReadonlyLeaf::ReadFile(input) => {
            from_value::<ReadFileInput>(&input).and_then(|parsed| run_read_file(lease, parsed))?
        }
        PreparedReadonlyLeaf::GlobSearch(input) => from_value::<GlobSearchInputValue>(&input)
            .and_then(|parsed| run_glob_search(lease, parsed))?,
        PreparedReadonlyLeaf::GrepSearch(input) => from_value::<GrepSearchInput>(&input)
            .and_then(|parsed| run_grep_search(lease, parsed))?,
        PreparedReadonlyLeaf::WorkspaceSnapshot(input) => {
            from_value::<WorkspaceSnapshotInput>(&input)
                .and_then(|parsed| run_workspace_snapshot(lease, parsed))?
        }
        PreparedReadonlyLeaf::ToolCacheStats => to_pretty_json(lease.cache().stats())?,
    };
    Ok(serde_json::from_str(&output).unwrap_or(Value::String(output)))
}

fn is_prepared_readonly_tool(name: &str) -> bool {
    matches!(
        name,
        "read_file" | "glob_search" | "grep_search" | "workspace_snapshot" | "tool_cache_stats"
    )
}

fn is_allowed_readonly_batch_tool(name: &str) -> bool {
    matches!(
        name,
        "read_file"
            | "read_many"
            | "glob_search"
            | "glob_many"
            | "grep_search"
            | "grep_many"
            | "workspace_snapshot"
            | "tool_cache_stats"
            | "mutation_preview"
            | "edit_many_preview"
            | "patch_plan"
    )
}

fn collect_snapshot_files(
    policy: &WorkspacePathPolicy,
    root: &Path,
    max_files: usize,
    files: &mut Vec<String>,
) -> bool {
    if files.len() >= max_files {
        return false;
    }
    let Ok(resolved_root) = policy.ensure_resolved_path(root) else {
        return false;
    };
    let Ok(metadata) = std::fs::metadata(&resolved_root) else {
        return false;
    };
    if metadata.is_file() {
        files.push(resolved_root.to_string_lossy().into_owned());
        return true;
    }
    if !metadata.is_dir() || should_skip_snapshot_dir(&resolved_root) {
        return true;
    }
    let Ok(entries) = std::fs::read_dir(&resolved_root) else {
        return false;
    };
    let mut complete = true;
    for entry in entries {
        let Ok(entry) = entry else {
            complete = false;
            continue;
        };
        if files.len() >= max_files {
            complete = false;
            break;
        }
        complete &= collect_snapshot_files(policy, &entry.path(), max_files, files);
    }
    complete
}

fn should_skip_snapshot_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            matches!(
                name,
                ".git"
                    | ".cowd"
                    | "target"
                    | "node_modules"
                    | "dist"
                    | "build"
                    | ".cache"
                    | "coverage"
            )
        })
}

#[derive(Debug, Serialize)]
struct BatchToolOutput {
    #[serde(rename = "type")]
    kind: String,
    #[serde(rename = "executionMode")]
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_mode: Option<String>,
    count: usize,
    #[serde(rename = "successCount")]
    success_count: usize,
    #[serde(rename = "errorCount")]
    error_count: usize,
    #[serde(rename = "partialSuccess")]
    partial_success: bool,
    results: Vec<BatchToolItemOutput>,
}

#[derive(Debug, Serialize)]
struct BatchToolItemOutput {
    index: usize,
    status: String,
    #[serde(rename = "durationMs")]
    duration_ms: u128,
    output: Option<Value>,
    error: Option<String>,
}

fn batch_output(kind: &str, results: Vec<BatchToolItemOutput>) -> BatchToolOutput {
    batch_output_internal(kind, None, results)
}

fn batch_output_with_mode(
    kind: &str,
    execution_mode: &str,
    results: Vec<BatchToolItemOutput>,
) -> BatchToolOutput {
    batch_output_internal(kind, Some(execution_mode.to_string()), results)
}

fn batch_output_internal(
    kind: &str,
    execution_mode: Option<String>,
    results: Vec<BatchToolItemOutput>,
) -> BatchToolOutput {
    let success_count = results
        .iter()
        .filter(|item| item.status == "success")
        .count();
    let error_count = results.len().saturating_sub(success_count);
    BatchToolOutput {
        kind: kind.to_string(),
        execution_mode,
        count: results.len(),
        success_count,
        error_count,
        partial_success: success_count > 0 && error_count > 0,
        results,
    }
}

fn run_ordered_batch<T, F>(
    items: Vec<T>,
    max_concurrency: Option<usize>,
    operation: F,
) -> Vec<BatchToolItemOutput>
where
    T: Clone + Send,
    F: Fn(T) -> Result<Value, String> + Sync,
{
    let concurrency = max_concurrency
        .unwrap_or(runtime_parallelism_ceiling())
        .clamp(1, runtime_parallelism_ceiling());
    let mut results: Vec<Option<BatchToolItemOutput>> =
        std::iter::repeat_with(|| None).take(items.len()).collect();

    for chunk_start in (0..items.len()).step_by(concurrency) {
        let chunk_end = chunk_start.saturating_add(concurrency).min(items.len());
        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for (offset, item) in items[chunk_start..chunk_end].iter().cloned().enumerate() {
                let operation = &operation;
                handles.push(scope.spawn(move || {
                    let started = Instant::now();
                    let result = operation(item);
                    let duration_ms = started.elapsed().as_millis();
                    (offset, duration_ms, result)
                }));
            }
            for handle in handles {
                match handle.join() {
                    Ok((offset, duration_ms, Ok(output))) => {
                        results[chunk_start + offset] = Some(BatchToolItemOutput {
                            index: chunk_start + offset,
                            status: String::from("success"),
                            duration_ms,
                            output: Some(output),
                            error: None,
                        });
                    }
                    Ok((offset, duration_ms, Err(error))) => {
                        results[chunk_start + offset] = Some(BatchToolItemOutput {
                            index: chunk_start + offset,
                            status: String::from("error"),
                            duration_ms,
                            output: None,
                            error: Some(error),
                        });
                    }
                    Err(_) => {
                        let offset = results[chunk_start..chunk_end]
                            .iter()
                            .position(Option::is_none)
                            .unwrap_or(0);
                        results[chunk_start + offset] = Some(BatchToolItemOutput {
                            index: chunk_start + offset,
                            status: String::from("error"),
                            duration_ms: 0,
                            output: None,
                            error: Some(String::from("batch item panicked")),
                        });
                    }
                }
            }
        });
    }

    results
        .into_iter()
        .enumerate()
        .map(|(index, item)| {
            item.unwrap_or(BatchToolItemOutput {
                index,
                status: String::from("error"),
                duration_ms: 0,
                output: None,
                error: Some(String::from("batch item did not complete")),
            })
        })
        .collect()
}

fn runtime_parallelism_ceiling() -> usize {
    std::env::var("COWD_TOOL_PARALLEL_CEILING")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| (1..=256).contains(value))
        .unwrap_or(42)
}

#[allow(clippy::needless_pass_by_value)]
fn run_web_fetch(input: WebFetchInput) -> Result<String, String> {
    to_pretty_json(execute_web_fetch(&input)?)
}

fn run_skill_install_plan(
    lease: &ToolHostLease,
    input: SkillInstallPlanInput,
) -> Result<String, String> {
    ensure_model_skill_source(lease, &input.source)?;
    let lifecycle = skill::SkillLifecycle::default_for_user().map_err(|error| error.to_string())?;
    let plan = lifecycle
        .plan(&input.source, lease.workspace_root())
        .map_err(|error| error.to_string())?;
    let next = if plan.installable {
        "Call skill_install_commit with this exact package_digest after reviewing warnings."
    } else {
        "Installation is blocked; do not fall back to shell or package-manager installation."
    };
    to_pretty_json(json!({
        "kind": "skill_install_plan",
        "schema_version": 1,
        "plan": plan,
        "next": next,
    }))
}

fn run_skill_install_commit(
    lease: &ToolHostLease,
    input: SkillInstallCommitInput,
) -> Result<String, String> {
    ensure_model_skill_source(lease, &input.source)?;
    let lifecycle = skill::SkillLifecycle::default_for_user().map_err(|error| error.to_string())?;
    let receipt = lifecycle
        .commit(
            &input.source,
            lease.workspace_root(),
            &input.expected_digest,
            input.allow_warnings,
            &format!("model:{}", lease.workspace_id()),
        )
        .map_err(|error| error.to_string())?;
    to_pretty_json(json!({
        "kind": "skill_install_receipt",
        "schema_version": 1,
        "receipt": receipt,
        "activation": "active_pointer_published",
        "execution": "none",
        "capabilities_granted": [],
    }))
}

fn ensure_model_skill_source(lease: &ToolHostLease, source: &str) -> Result<(), String> {
    let source = source.trim();
    if source.starts_with("https://github.com/") || source.starts_with("github://") {
        return Ok(());
    }
    let candidate = PathBuf::from(source);
    let candidate = if candidate.is_absolute() {
        candidate
    } else {
        lease.workspace_root().join(candidate)
    };
    let canonical = candidate
        .canonicalize()
        .map_err(|error| format!("invalid local Skill source: {error}"))?;
    let workspace = lease
        .workspace_root()
        .canonicalize()
        .map_err(|error| format!("workspace root unavailable: {error}"))?;
    if !canonical.starts_with(workspace) {
        return Err(
            "model Skill acquisition is limited to the current workspace or explicit GitHub sources"
                .to_string(),
        );
    }
    Ok(())
}

fn run_skill_status(input: SkillStatusInput) -> Result<String, String> {
    let store = skill::ManagedSkillStore::default_for_user().map_err(|error| error.to_string())?;
    to_pretty_json(
        store
            .status(&input.skill_id)
            .map_err(|error| error.to_string())?,
    )
}

fn run_skill_rollback(lease: &ToolHostLease, input: SkillRollbackInput) -> Result<String, String> {
    let store = skill::ManagedSkillStore::default_for_user().map_err(|error| error.to_string())?;
    let receipt = store
        .rollback(
            &input.skill_id,
            &input.revision,
            &format!("model:{}", lease.workspace_id()),
        )
        .map_err(|error| error.to_string())?;
    to_pretty_json(json!({
        "kind": "skill_rollback_receipt",
        "schema_version": 1,
        "receipt": receipt,
        "execution": "none",
    }))
}

fn run_skill_deactivate(lease: &ToolHostLease, input: SkillStatusInput) -> Result<String, String> {
    let store = skill::ManagedSkillStore::default_for_user().map_err(|error| error.to_string())?;
    let previous = store
        .deactivate(&input.skill_id, &format!("model:{}", lease.workspace_id()))
        .map_err(|error| error.to_string())?;
    to_pretty_json(json!({
        "kind": "skill_deactivation_receipt",
        "schema_version": 1,
        "skill_id": input.skill_id,
        "previous_active": previous,
        "revisions_retained": true,
        "execution": "none",
    }))
}

#[allow(clippy::needless_pass_by_value)]
fn run_web_search(input: WebSearchInput) -> Result<String, String> {
    to_pretty_json(execute_web_search(&input)?)
}

fn run_todo_write(lease: &ToolHostLease, input: TodoWriteInput) -> Result<String, String> {
    to_pretty_json(execute_todo_write(lease.workspace_root(), input)?)
}

fn run_tool_search(lease: &ToolHostLease, input: ToolSearchInput) -> Result<String, String> {
    to_pretty_json(execute_tool_search(lease, input))
}

fn run_current_time() -> Result<String, String> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now();
    let since_epoch = now
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock is before the Unix epoch: {error}"))?;
    let seconds = since_epoch.as_secs();
    let millis = since_epoch.subsec_millis();
    let datetime = chrono_like_iso8601(seconds);
    to_pretty_json(serde_json::json!({
        "kind": "current_time",
        "iso8601_utc": format!("{datetime}.{:03}Z", millis),
        "unix_seconds": seconds,
        "unix_millis": seconds.saturating_mul(1000).saturating_add(u64::from(millis)),
        "timezone": current_timezone_name(),
    }))
}

fn chrono_like_iso8601(unix_seconds: u64) -> String {
    // days since epoch (civil algorithm)
    let days = unix_seconds / 86_400;
    let seconds_of_day = unix_seconds % 86_400;
    let (year, month, day) = civil_from_days(days as i64);
    let hour = seconds_of_day / 3600;
    let minute = (seconds_of_day % 3600) / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}")
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (if month <= 2 { year + 1 } else { year }, month, day)
}

fn current_timezone_name() -> String {
    std::env::var("TZ")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| {
            std::env::var("COWD_TIMEZONE")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "UTC".to_string())
        })
}

fn run_get_context_remaining(input: &Value) -> Result<String, String> {
    // The tools crate cannot see the active conversation ledger. The Gateway
    // runtime intercepts this tool and answers from the live execution store;
    // this branch only exists for offline/test hosts and fails closed rather
    // than inventing a window.
    let detail = input
        .get("detail")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("summary");
    to_pretty_json(serde_json::json!({
        "kind": "get_context_remaining",
        "status": "delegated",
        "detail": detail,
        "message": "context utilization is owned by the active Runtime; this host has no session ledger",
        "context_window_tokens": null,
        "input_tokens": null,
        "remaining_tokens": null,
        "usage_percent_bp": null
    }))
}

fn run_notebook_edit(lease: &ToolHostLease, input: NotebookEditInput) -> Result<String, String> {
    to_pretty_json(execute_notebook_edit(lease.path_policy(), input)?)
}

fn run_sleep(input: SleepInput) -> Result<String, String> {
    to_pretty_json(execute_sleep(input)?)
}

fn run_brief(lease: &ToolHostLease, input: BriefInput) -> Result<String, String> {
    to_pretty_json(execute_brief(lease.path_policy(), input)?)
}

fn run_config(lease: &ToolHostLease, input: ConfigInput) -> Result<String, String> {
    to_pretty_json(execute_config(lease.workspace_root(), input)?)
}

fn run_enter_plan_mode(lease: &ToolHostLease, input: EnterPlanModeInput) -> Result<String, String> {
    to_pretty_json(execute_enter_plan_mode(lease.workspace_root(), input)?)
}

fn run_exit_plan_mode(lease: &ToolHostLease, input: ExitPlanModeInput) -> Result<String, String> {
    to_pretty_json(execute_exit_plan_mode(lease.workspace_root(), input)?)
}

fn run_structured_output(input: StructuredOutputInput) -> Result<String, String> {
    to_pretty_json(execute_structured_output(input)?)
}

fn run_repl(lease: &ToolHostLease, input: ReplInput) -> Result<String, String> {
    to_pretty_json(execute_repl(lease.workspace_root(), input)?)
}

fn run_powershell(lease: &ToolHostLease, input: PowerShellInput) -> Result<String, String> {
    to_pretty_json(
        execute_powershell(lease.workspace_root(), input).map_err(|error| error.to_string())?,
    )
}

fn to_pretty_json<T: serde::Serialize>(value: T) -> Result<String, String> {
    serde_json::to_string_pretty(&value).map_err(|error| error.to_string())
}

#[allow(clippy::needless_pass_by_value)]
fn io_to_string(error: std::io::Error) -> String {
    error.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReadFileInput {
    path: String,
    offset: Option<usize>,
    limit: Option<usize>,
    #[serde(default)]
    complete: bool,
}

#[derive(Debug, Deserialize)]
struct ReadManyInput {
    files: Vec<ReadFileInput>,
    max_concurrency: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct WriteFileInput {
    path: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct EditFileInput {
    path: String,
    old_string: String,
    new_string: String,
    replace_all: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GlobSearchInputValue {
    pattern: String,
    path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GlobManyInput {
    patterns: Vec<GlobSearchInputValue>,
    max_concurrency: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct GrepManyInput {
    searches: Vec<GrepSearchInput>,
    max_concurrency: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkspaceSnapshotInput {
    include_git: Option<bool>,
    include_files: Option<bool>,
    roots: Option<Vec<String>>,
    max_files: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
struct ToolBatchReadonlyInput {
    calls: Vec<ToolBatchReadonlyCallInput>,
    max_concurrency: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
struct ToolBatchReadonlyCallInput {
    name: String,
    input: Value,
}

#[derive(Debug, Deserialize)]
struct WebFetchInput {
    url: String,
    prompt: String,
    #[serde(default)]
    allowed_domains: Option<Vec<String>>,
    #[serde(default)]
    blocked_domains: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct TodoWriteInput {
    todos: Vec<TodoItem>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
struct TodoItem {
    content: String,
    #[serde(rename = "activeForm")]
    active_form: String,
    status: TodoStatus,
    #[serde(default)]
    priority: TodoPriority,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
enum TodoPriority {
    #[default]
    Medium,
    Low,
    High,
    Critical,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TodoStatus {
    Pending,
    InProgress,
    Completed,
}

#[derive(Debug, Deserialize)]
struct ToolSearchInput {
    query: String,
    max_results: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct NotebookEditInput {
    notebook_path: String,
    cell_id: Option<String>,
    new_source: Option<String>,
    cell_type: Option<NotebookCellType>,
    edit_mode: Option<NotebookEditMode>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum NotebookCellType {
    Code,
    Markdown,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum NotebookEditMode {
    Replace,
    Insert,
    Delete,
}

#[derive(Debug, Deserialize)]
struct SleepInput {
    duration_ms: u64,
}

#[derive(Debug, Deserialize)]
struct BriefInput {
    message: String,
    attachments: Option<Vec<String>>,
    status: BriefStatus,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum BriefStatus {
    Normal,
    Proactive,
}

#[derive(Debug, Deserialize)]
struct ConfigInput {
    setting: String,
    value: Option<ConfigValue>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct EnterPlanModeInput {}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct ExitPlanModeInput {}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ConfigValue {
    String(String),
    Bool(bool),
    Number(f64),
}

#[derive(Debug, Deserialize)]
#[serde(transparent)]
struct StructuredOutputInput(BTreeMap<String, Value>);

#[derive(Debug, Deserialize)]
struct ReplInput {
    code: String,
    language: String,
    timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PowerShellInput {
    command: String,
    timeout_ms: Option<u64>,
    description: Option<String>,
    run_in_background: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillInstallPlanInput {
    source: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillInstallCommitInput {
    source: String,
    expected_digest: String,
    #[serde(default)]
    allow_warnings: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillStatusInput {
    skill_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillRollbackInput {
    skill_id: String,
    revision: String,
}

#[derive(Debug, Deserialize)]
struct AskUserQuestionInput {
    question: String,
    #[serde(default)]
    options: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct LspInput {
    action: String,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    line: Option<u32>,
    #[serde(default)]
    character: Option<u32>,
    #[serde(default)]
    query: Option<String>,
}

#[derive(Debug, Deserialize)]
struct McpResourceInput {
    #[serde(default)]
    server: Option<String>,
    #[serde(default)]
    uri: Option<String>,
}

#[derive(Debug, Deserialize)]
struct McpAuthInput {
    server: String,
}

#[derive(Debug, Deserialize)]
struct RemoteTriggerInput {
    url: String,
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    headers: Option<Value>,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct McpToolInput {
    server: String,
    tool: String,
    #[serde(default)]
    arguments: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct TestingPermissionInput {
    action: String,
}

#[derive(Debug, Serialize)]
struct WebFetchOutput {
    bytes: usize,
    code: u16,
    #[serde(rename = "codeText")]
    code_text: String,
    result: String,
    #[serde(rename = "networkPolicy")]
    network_policy: crate::network_policy::NetworkPolicyReceipt,
    #[serde(rename = "durationMs")]
    duration_ms: u128,
    url: String,
}

#[derive(Debug, Serialize)]
struct TodoWriteOutput {
    #[serde(rename = "oldTodos")]
    old_todos: Vec<TodoItem>,
    #[serde(rename = "newTodos")]
    new_todos: Vec<TodoItem>,
    #[serde(rename = "verificationNudgeNeeded")]
    verification_nudge_needed: Option<bool>,
}

#[derive(Debug, Serialize)]
struct NotebookEditOutput {
    new_source: String,
    cell_id: Option<String>,
    cell_type: Option<NotebookCellType>,
    language: String,
    edit_mode: String,
    error: Option<String>,
    notebook_path: String,
    original_file: String,
    updated_file: String,
}

#[derive(Debug, Serialize)]
struct SleepOutput {
    duration_ms: u64,
    message: String,
}

#[derive(Debug, Serialize)]
struct BriefOutput {
    message: String,
    attachments: Option<Vec<ResolvedAttachment>>,
    #[serde(rename = "sentAt")]
    sent_at: String,
}

#[derive(Debug, Serialize)]
struct ResolvedAttachment {
    path: String,
    size: u64,
    #[serde(rename = "isImage")]
    is_image: bool,
}

#[derive(Debug, Serialize)]
struct ConfigOutput {
    success: bool,
    operation: Option<String>,
    setting: Option<String>,
    value: Option<Value>,
    #[serde(rename = "previousValue")]
    previous_value: Option<Value>,
    #[serde(rename = "newValue")]
    new_value: Option<Value>,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PlanModeState {
    #[serde(rename = "hadLocalOverride")]
    had_local_override: bool,
    #[serde(rename = "previousLocalMode")]
    previous_local_mode: Option<Value>,
}

#[derive(Debug, Serialize)]
#[allow(clippy::struct_excessive_bools)]
struct PlanModeOutput {
    success: bool,
    operation: String,
    changed: bool,
    active: bool,
    managed: bool,
    message: String,
    #[serde(rename = "settingsPath")]
    settings_path: String,
    #[serde(rename = "statePath")]
    state_path: String,
    #[serde(rename = "previousLocalMode")]
    previous_local_mode: Option<Value>,
    #[serde(rename = "currentLocalMode")]
    current_local_mode: Option<Value>,
}

#[derive(Debug, Clone)]
pub(crate) struct SearchableToolSpec {
    pub(crate) name: String,
    pub(crate) description: String,
}

#[derive(Debug, Serialize)]
struct StructuredOutputResult {
    data: String,
    structured_output: BTreeMap<String, Value>,
}

#[derive(Debug, Serialize)]
struct ReplOutput {
    language: String,
    stdout: String,
    stderr: String,
    #[serde(rename = "exitCode")]
    exit_code: i32,
    #[serde(rename = "durationMs")]
    duration_ms: u128,
}

fn execute_web_fetch(input: &WebFetchInput) -> Result<WebFetchOutput, String> {
    let started = Instant::now();
    let policy = crate::network_policy::NetworkDomainPolicy::from_env();
    let policy_receipt = policy.merge_call_filters(
        input.allowed_domains.as_deref(),
        input.blocked_domains.as_deref(),
    );
    if policy_receipt.denied || policy_receipt.requires_approval {
        return Ok(WebFetchOutput {
            bytes: 0,
            code: 0,
            code_text: String::new(),
            result: if policy_receipt.denied {
                "Network domain policy denied the fetch request."
            } else {
                "Network domain policy requires approval before fetching this URL."
            }
            .to_string(),
            network_policy: policy_receipt,
            duration_ms: started.elapsed().as_millis(),
            url: input.url.clone(),
        });
    }
    let client = build_http_client()?;
    let request_url = normalize_fetch_url(&input.url)?;
    let url_receipt = policy.enforce_url(&request_url)?;
    if url_receipt.denied || url_receipt.requires_approval {
        return Ok(WebFetchOutput {
            bytes: 0,
            code: 0,
            code_text: String::new(),
            result: if url_receipt.denied {
                "Network domain policy blocked the requested URL."
            } else {
                "Network domain policy requires approval for the requested URL."
            }
            .to_string(),
            network_policy: url_receipt,
            duration_ms: started.elapsed().as_millis(),
            url: input.url.clone(),
        });
    }
    let response = client
        .get(request_url.clone())
        .send()
        .map_err(|error| error.to_string())?;

    let status = response.status();
    let final_url = response.url().to_string();
    let code = status.as_u16();
    let code_text = status.canonical_reason().unwrap_or("Unknown").to_string();
    if let Ok(final_receipt) = policy.enforce_url(&final_url) {
        if final_receipt.denied || final_receipt.requires_approval {
            return Ok(WebFetchOutput {
                bytes: 0,
                code,
                code_text,
                result: "Network domain policy blocked the final redirect target.".to_string(),
                network_policy: final_receipt,
                duration_ms: started.elapsed().as_millis(),
                url: final_url,
            });
        }
    }
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = response.text().map_err(|error| error.to_string())?;
    let bytes = body.len();
    let normalized = normalize_fetched_content(&body, &content_type);
    let result = summarize_web_fetch(&final_url, &input.prompt, &normalized, &body, &content_type);

    Ok(WebFetchOutput {
        bytes,
        code,
        code_text,
        result,
        network_policy: policy_receipt,
        duration_ms: started.elapsed().as_millis(),
        url: final_url,
    })
}

fn build_http_client() -> Result<Client, String> {
    Client::builder()
        .timeout(Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::limited(10))
        .user_agent(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
             (KHTML, like Gecko) Chrome/131.0 Safari/537.36 Cowd/0.9",
        )
        .build()
        .map_err(|error| error.to_string())
}

fn normalize_fetch_url(url: &str) -> Result<String, String> {
    let parsed = reqwest::Url::parse(url).map_err(|error| error.to_string())?;
    if parsed.scheme() == "http" {
        let host = parsed.host_str().unwrap_or_default();
        if host != "localhost" && host != "127.0.0.1" && host != "::1" {
            let mut upgraded = parsed;
            upgraded
                .set_scheme("https")
                .map_err(|()| String::from("failed to upgrade URL to https"))?;
            return Ok(upgraded.to_string());
        }
    }
    Ok(parsed.to_string())
}

fn normalize_fetched_content(body: &str, content_type: &str) -> String {
    if content_type.contains("html") {
        html_to_text(body)
    } else {
        body.trim().to_string()
    }
}

fn summarize_web_fetch(
    url: &str,
    prompt: &str,
    content: &str,
    raw_body: &str,
    content_type: &str,
) -> String {
    let lower_prompt = prompt.to_lowercase();
    let compact = collapse_whitespace(content);

    let detail = if lower_prompt.contains("title") {
        extract_title(content, raw_body, content_type).map_or_else(
            || preview_text(&compact, 600),
            |title| format!("Title: {title}"),
        )
    } else if lower_prompt.contains("summary") || lower_prompt.contains("summarize") {
        preview_text(&compact, 900)
    } else {
        let preview = preview_text(&compact, 900);
        format!("Prompt: {prompt}\nContent preview:\n{preview}")
    };

    format!("Fetched {url}\n{detail}")
}

fn extract_title(content: &str, raw_body: &str, content_type: &str) -> Option<String> {
    if content_type.contains("html") {
        let lowered = raw_body.to_lowercase();
        if let Some(start) = lowered.find("<title>") {
            let after = start + "<title>".len();
            if let Some(end_rel) = lowered[after..].find("</title>") {
                let title =
                    collapse_whitespace(&decode_html_entities(&raw_body[after..after + end_rel]));
                if !title.is_empty() {
                    return Some(title);
                }
            }
        }
    }

    for line in content.lines() {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    None
}

fn html_to_text(html: &str) -> String {
    let mut text = String::with_capacity(html.len());
    let mut in_tag = false;
    let mut previous_was_space = false;

    for ch in html.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if in_tag => {}
            '&' => {
                text.push('&');
                previous_was_space = false;
            }
            ch if ch.is_whitespace() => {
                if !previous_was_space {
                    text.push(' ');
                    previous_was_space = true;
                }
            }
            _ => {
                text.push(ch);
                previous_was_space = false;
            }
        }
    }

    collapse_whitespace(&decode_html_entities(&text))
}

fn decode_html_entities(input: &str) -> String {
    input
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ")
}

fn collapse_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn preview_text(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    let shortened = input.chars().take(max_chars).collect::<String>();
    format!("{}…", shortened.trim_end())
}

fn execute_todo_write(
    workspace_root: &Path,
    input: TodoWriteInput,
) -> Result<TodoWriteOutput, String> {
    validate_todos(&input.todos)?;
    let store_path = todo_store_path(workspace_root);
    let old_todos = if store_path.exists() {
        serde_json::from_str::<Vec<TodoItem>>(
            &std::fs::read_to_string(&store_path).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?
    } else {
        Vec::new()
    };

    let all_done = input
        .todos
        .iter()
        .all(|todo| matches!(todo.status, TodoStatus::Completed));
    let persisted = if all_done {
        Vec::new()
    } else {
        input.todos.clone()
    };

    if let Some(parent) = store_path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    std::fs::write(
        &store_path,
        serde_json::to_string_pretty(&persisted).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;

    let verification_nudge_needed = (all_done
        && input.todos.len() >= 3
        && !input
            .todos
            .iter()
            .any(|todo| todo.content.to_lowercase().contains("verif")))
    .then_some(true);

    Ok(TodoWriteOutput {
        old_todos,
        new_todos: input.todos,
        verification_nudge_needed,
    })
}

fn validate_todos(todos: &[TodoItem]) -> Result<(), String> {
    if todos.is_empty() {
        return Err(String::from("todos must not be empty"));
    }
    // Allow multiple in_progress items for parallel workflows
    if todos.iter().any(|todo| todo.content.trim().is_empty()) {
        return Err(String::from("todo content must not be empty"));
    }
    if todos.iter().any(|todo| todo.active_form.trim().is_empty()) {
        return Err(String::from("todo activeForm must not be empty"));
    }
    Ok(())
}

fn todo_store_path(workspace_root: &Path) -> std::path::PathBuf {
    if let Ok(path) = std::env::var("COWD_TODO_STORE") {
        let path = std::path::PathBuf::from(path);
        return if path.is_absolute() {
            path
        } else {
            workspace_root.join(path)
        };
    }
    workspace_root.join(".cowd-todos.json")
}

#[allow(clippy::needless_pass_by_value)]
fn execute_tool_search(
    lease: &ToolHostLease,
    input: ToolSearchInput,
) -> harness_contract::tool::ToolDiscoveryReceipt {
    lease.search(&input.query, input.max_results.unwrap_or(5))
}

pub(crate) fn search_tool_specs(
    query: &str,
    max_results: usize,
    specs: &[SearchableToolSpec],
) -> Vec<String> {
    let lowered = query.to_lowercase();
    if let Some(selection) = lowered.strip_prefix("select:") {
        return selection
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .filter_map(|wanted| {
                let wanted = canonical_tool_token(wanted);
                specs
                    .iter()
                    .find(|spec| canonical_tool_token(&spec.name) == wanted)
                    .map(|spec| spec.name.clone())
            })
            .take(max_results)
            .collect();
    }

    let mut required = Vec::new();
    let mut optional = Vec::new();
    for term in lowered.split_whitespace() {
        if let Some(rest) = term.strip_prefix('+') {
            if !rest.is_empty() {
                required.push(rest);
            }
        } else {
            optional.push(term);
        }
    }
    let terms = if required.is_empty() {
        optional.clone()
    } else {
        required.iter().chain(optional.iter()).copied().collect()
    };

    let mut scored = specs
        .iter()
        .filter_map(|spec| {
            let name = spec.name.to_lowercase();
            let canonical_name = canonical_tool_token(&spec.name);
            let normalized_description = normalize_tool_search_query(&spec.description);
            let haystack = format!(
                "{name} {} {canonical_name}",
                spec.description.to_lowercase()
            );
            let normalized_haystack = format!("{canonical_name} {normalized_description}");
            if required.iter().any(|term| !haystack.contains(term)) {
                return None;
            }

            let mut score = 0_i32;
            for term in &terms {
                let canonical_term = canonical_tool_token(term);
                if haystack.contains(term) {
                    score += 2;
                }
                if name == *term {
                    score += 8;
                }
                if name.contains(term) {
                    score += 4;
                }
                if canonical_name == canonical_term {
                    score += 12;
                }
                if normalized_haystack.contains(&canonical_term) {
                    score += 3;
                }
            }

            if score == 0 && !lowered.is_empty() {
                return None;
            }
            Some((score, spec.name.clone()))
        })
        .collect::<Vec<_>>();

    scored.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    scored
        .into_iter()
        .map(|(_, name)| name)
        .take(max_results)
        .collect()
}

pub(crate) fn normalize_tool_search_query(query: &str) -> String {
    query
        .trim()
        .split(|ch: char| ch.is_whitespace() || ch == ',')
        .filter(|term| !term.is_empty())
        .map(canonical_tool_token)
        .collect::<Vec<_>>()
        .join(" ")
}

fn canonical_tool_token(value: &str) -> String {
    let mut canonical = value
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .flat_map(char::to_lowercase)
        .collect::<String>();
    if let Some(stripped) = canonical.strip_suffix("tool") {
        canonical = stripped.to_string();
    }
    canonical
}

#[allow(clippy::too_many_lines)]
fn execute_notebook_edit(
    policy: &WorkspacePathPolicy,
    input: NotebookEditInput,
) -> Result<NotebookEditOutput, String> {
    let path = policy.resolve(&input.notebook_path).map_err(io_to_string)?;
    if path.extension().and_then(|ext| ext.to_str()) != Some("ipynb") {
        return Err(String::from(
            "File must be a Jupyter notebook (.ipynb file).",
        ));
    }

    let original_file = std::fs::read_to_string(&path).map_err(|error| error.to_string())?;
    let mut notebook: serde_json::Value =
        serde_json::from_str(&original_file).map_err(|error| error.to_string())?;
    let language = notebook
        .get("metadata")
        .and_then(|metadata| metadata.get("kernelspec"))
        .and_then(|kernelspec| kernelspec.get("language"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("python")
        .to_string();
    let cells = notebook
        .get_mut("cells")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or_else(|| String::from("Notebook cells array not found"))?;

    let edit_mode = input.edit_mode.unwrap_or(NotebookEditMode::Replace);
    let target_index = match input.cell_id.as_deref() {
        Some(cell_id) => Some(resolve_cell_index(cells, Some(cell_id), edit_mode)?),
        None if matches!(
            edit_mode,
            NotebookEditMode::Replace | NotebookEditMode::Delete
        ) =>
        {
            Some(resolve_cell_index(cells, None, edit_mode)?)
        }
        None => None,
    };
    let resolved_cell_type = match edit_mode {
        NotebookEditMode::Delete => None,
        NotebookEditMode::Insert => Some(input.cell_type.unwrap_or(NotebookCellType::Code)),
        NotebookEditMode::Replace => Some(input.cell_type.unwrap_or_else(|| {
            target_index
                .and_then(|index| cells.get(index))
                .and_then(cell_kind)
                .unwrap_or(NotebookCellType::Code)
        })),
    };
    let new_source = require_notebook_source(input.new_source, edit_mode)?;

    let cell_id = match edit_mode {
        NotebookEditMode::Insert => {
            let resolved_cell_type = resolved_cell_type
                .ok_or_else(|| String::from("insert mode requires a cell type"))?;
            let new_id = make_cell_id(cells.len());
            let new_cell = build_notebook_cell(&new_id, resolved_cell_type, &new_source);
            let insert_at = target_index.map_or(cells.len(), |index| index + 1);
            cells.insert(insert_at, new_cell);
            cells
                .get(insert_at)
                .and_then(|cell| cell.get("id"))
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string)
        }
        NotebookEditMode::Delete => {
            let idx = target_index
                .ok_or_else(|| String::from("delete mode requires a target cell index"))?;
            let removed = cells.remove(idx);
            removed
                .get("id")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string)
        }
        NotebookEditMode::Replace => {
            let resolved_cell_type = resolved_cell_type
                .ok_or_else(|| String::from("replace mode requires a cell type"))?;
            let idx = target_index
                .ok_or_else(|| String::from("replace mode requires a target cell index"))?;
            let cell = cells
                .get_mut(idx)
                .ok_or_else(|| String::from("Cell index out of range"))?;
            cell["source"] = serde_json::Value::Array(source_lines(&new_source));
            cell["cell_type"] = serde_json::Value::String(match resolved_cell_type {
                NotebookCellType::Code => String::from("code"),
                NotebookCellType::Markdown => String::from("markdown"),
            });
            match resolved_cell_type {
                NotebookCellType::Code => {
                    if !cell.get("outputs").is_some_and(serde_json::Value::is_array) {
                        cell["outputs"] = json!([]);
                    }
                    if cell.get("execution_count").is_none() {
                        cell["execution_count"] = serde_json::Value::Null;
                    }
                }
                NotebookCellType::Markdown => {
                    if let Some(object) = cell.as_object_mut() {
                        object.remove("outputs");
                        object.remove("execution_count");
                    }
                }
            }
            cell.get("id")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string)
        }
    };

    let updated_file =
        serde_json::to_string_pretty(&notebook).map_err(|error| error.to_string())?;
    std::fs::write(&path, &updated_file).map_err(|error| error.to_string())?;

    Ok(NotebookEditOutput {
        new_source,
        cell_id,
        cell_type: resolved_cell_type,
        language,
        edit_mode: format_notebook_edit_mode(edit_mode),
        error: None,
        notebook_path: path.display().to_string(),
        original_file,
        updated_file,
    })
}

fn require_notebook_source(
    source: Option<String>,
    edit_mode: NotebookEditMode,
) -> Result<String, String> {
    match edit_mode {
        NotebookEditMode::Delete => Ok(source.unwrap_or_default()),
        NotebookEditMode::Insert | NotebookEditMode::Replace => source
            .ok_or_else(|| String::from("new_source is required for insert and replace edits")),
    }
}

fn build_notebook_cell(cell_id: &str, cell_type: NotebookCellType, source: &str) -> Value {
    let mut cell = json!({
        "cell_type": match cell_type {
            NotebookCellType::Code => "code",
            NotebookCellType::Markdown => "markdown",
        },
        "id": cell_id,
        "metadata": {},
        "source": source_lines(source),
    });
    if let Some(object) = cell.as_object_mut() {
        match cell_type {
            NotebookCellType::Code => {
                object.insert(String::from("outputs"), json!([]));
                object.insert(String::from("execution_count"), Value::Null);
            }
            NotebookCellType::Markdown => {}
        }
    }
    cell
}

fn cell_kind(cell: &serde_json::Value) -> Option<NotebookCellType> {
    cell.get("cell_type")
        .and_then(serde_json::Value::as_str)
        .map(|kind| {
            if kind == "markdown" {
                NotebookCellType::Markdown
            } else {
                NotebookCellType::Code
            }
        })
}

const MAX_SLEEP_DURATION_MS: u64 = 300_000;

#[allow(clippy::needless_pass_by_value)]
fn execute_sleep(input: SleepInput) -> Result<SleepOutput, String> {
    if input.duration_ms > MAX_SLEEP_DURATION_MS {
        return Err(format!(
            "duration_ms {} exceeds maximum allowed sleep of {MAX_SLEEP_DURATION_MS}ms",
            input.duration_ms,
        ));
    }
    std::thread::sleep(Duration::from_millis(input.duration_ms));
    Ok(SleepOutput {
        duration_ms: input.duration_ms,
        message: format!("Slept for {}ms", input.duration_ms),
    })
}

fn execute_brief(policy: &WorkspacePathPolicy, input: BriefInput) -> Result<BriefOutput, String> {
    if input.message.trim().is_empty() {
        return Err(String::from("message must not be empty"));
    }

    let attachments = input
        .attachments
        .as_ref()
        .map(|paths| {
            paths
                .iter()
                .map(|path| resolve_attachment(policy, path))
                .collect::<Result<Vec<_>, String>>()
        })
        .transpose()?;

    let message = match input.status {
        BriefStatus::Normal | BriefStatus::Proactive => input.message,
    };

    Ok(BriefOutput {
        message,
        attachments,
        sent_at: iso8601_timestamp(),
    })
}

fn resolve_attachment(
    policy: &WorkspacePathPolicy,
    path: &str,
) -> Result<ResolvedAttachment, String> {
    let resolved = policy.resolve(path).map_err(io_to_string)?;
    let metadata = std::fs::metadata(&resolved).map_err(|error| error.to_string())?;
    Ok(ResolvedAttachment {
        path: resolved.display().to_string(),
        size: metadata.len(),
        is_image: is_image_path(&resolved),
    })
}

fn is_image_path(path: &Path) -> bool {
    matches!(
        path.extension()
            .and_then(|ext| ext.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("png" | "jpg" | "jpeg" | "gif" | "webp" | "bmp" | "svg")
    )
}

fn execute_config(workspace_root: &Path, input: ConfigInput) -> Result<ConfigOutput, String> {
    let setting = input.setting.trim();
    if setting.is_empty() {
        return Err(String::from("setting must not be empty"));
    }
    let Some(spec) = supported_config_setting(setting) else {
        return Ok(ConfigOutput {
            success: false,
            operation: None,
            setting: None,
            value: None,
            previous_value: None,
            new_value: None,
            error: Some(format!("Unknown setting: \"{setting}\"")),
        });
    };

    let path = config_file_for_scope(spec.scope, workspace_root)?;
    let mut document = read_json_object(&path)?;

    if let Some(value) = input.value {
        let normalized = normalize_config_value(spec, value)?;
        let previous_value = get_nested_value(&document, spec.path).cloned();
        set_nested_value(&mut document, spec.path, normalized.clone());
        write_json_object(&path, &document)?;
        Ok(ConfigOutput {
            success: true,
            operation: Some(String::from("set")),
            setting: Some(setting.to_string()),
            value: Some(normalized.clone()),
            previous_value,
            new_value: Some(normalized),
            error: None,
        })
    } else {
        Ok(ConfigOutput {
            success: true,
            operation: Some(String::from("get")),
            setting: Some(setting.to_string()),
            value: get_nested_value(&document, spec.path).cloned(),
            previous_value: None,
            new_value: None,
            error: None,
        })
    }
}

const PERMISSION_DEFAULT_MODE_PATH: &[&str] = &["permissions", "default_mode"];

fn execute_enter_plan_mode(
    workspace_root: &Path,
    _input: EnterPlanModeInput,
) -> Result<PlanModeOutput, String> {
    let settings_path = config_file_for_scope(ConfigScope::Settings, workspace_root)?;
    let state_path = plan_mode_state_file(workspace_root)?;
    let mut document = read_json_object(&settings_path)?;
    let current_local_mode = get_nested_value(&document, PERMISSION_DEFAULT_MODE_PATH).cloned();
    let current_is_plan =
        matches!(current_local_mode.as_ref(), Some(Value::String(value)) if value == "read-only");

    if let Some(state) = read_plan_mode_state(&state_path)? {
        if current_is_plan {
            return Ok(PlanModeOutput {
                success: true,
                operation: String::from("enter"),
                changed: false,
                active: true,
                managed: true,
                message: String::from("Plan mode override is already active for this worktree."),
                settings_path: settings_path.display().to_string(),
                state_path: state_path.display().to_string(),
                previous_local_mode: state.previous_local_mode,
                current_local_mode,
            });
        }
        clear_plan_mode_state(&state_path)?;
    }

    if current_is_plan {
        return Ok(PlanModeOutput {
            success: true,
            operation: String::from("enter"),
            changed: false,
            active: true,
            managed: false,
            message: String::from(
                "Worktree-local plan mode is already enabled outside enter_plan_mode; leaving it unchanged.",
            ),
            settings_path: settings_path.display().to_string(),
            state_path: state_path.display().to_string(),
            previous_local_mode: None,
            current_local_mode,
        });
    }

    let state = PlanModeState {
        had_local_override: current_local_mode.is_some(),
        previous_local_mode: current_local_mode.clone(),
    };
    write_plan_mode_state(&state_path, &state)?;
    set_nested_value(
        &mut document,
        PERMISSION_DEFAULT_MODE_PATH,
        Value::String(String::from("read-only")),
    );
    write_json_object(&settings_path, &document)?;

    Ok(PlanModeOutput {
        success: true,
        operation: String::from("enter"),
        changed: true,
        active: true,
        managed: true,
        message: String::from("Enabled worktree-local plan mode override."),
        settings_path: settings_path.display().to_string(),
        state_path: state_path.display().to_string(),
        previous_local_mode: state.previous_local_mode,
        current_local_mode: get_nested_value(&document, PERMISSION_DEFAULT_MODE_PATH).cloned(),
    })
}

fn execute_exit_plan_mode(
    workspace_root: &Path,
    _input: ExitPlanModeInput,
) -> Result<PlanModeOutput, String> {
    let settings_path = config_file_for_scope(ConfigScope::Settings, workspace_root)?;
    let state_path = plan_mode_state_file(workspace_root)?;
    let mut document = read_json_object(&settings_path)?;
    let current_local_mode = get_nested_value(&document, PERMISSION_DEFAULT_MODE_PATH).cloned();
    let current_is_plan =
        matches!(current_local_mode.as_ref(), Some(Value::String(value)) if value == "read-only");

    let Some(state) = read_plan_mode_state(&state_path)? else {
        return Ok(PlanModeOutput {
            success: true,
            operation: String::from("exit"),
            changed: false,
            active: current_is_plan,
            managed: false,
            message: String::from("No enter_plan_mode override is active for this worktree."),
            settings_path: settings_path.display().to_string(),
            state_path: state_path.display().to_string(),
            previous_local_mode: None,
            current_local_mode,
        });
    };

    if !current_is_plan {
        clear_plan_mode_state(&state_path)?;
        return Ok(PlanModeOutput {
            success: true,
            operation: String::from("exit"),
            changed: false,
            active: false,
            managed: false,
            message: String::from(
                "Cleared stale enter_plan_mode state because plan mode was already changed outside the tool.",
            ),
            settings_path: settings_path.display().to_string(),
            state_path: state_path.display().to_string(),
            previous_local_mode: state.previous_local_mode,
            current_local_mode,
        });
    }

    if state.had_local_override {
        if let Some(previous_local_mode) = state.previous_local_mode.clone() {
            set_nested_value(
                &mut document,
                PERMISSION_DEFAULT_MODE_PATH,
                previous_local_mode,
            );
        } else {
            remove_nested_value(&mut document, PERMISSION_DEFAULT_MODE_PATH);
        }
    } else {
        remove_nested_value(&mut document, PERMISSION_DEFAULT_MODE_PATH);
    }
    write_json_object(&settings_path, &document)?;
    clear_plan_mode_state(&state_path)?;

    Ok(PlanModeOutput {
        success: true,
        operation: String::from("exit"),
        changed: true,
        active: false,
        managed: false,
        message: String::from("Restored the prior worktree-local plan mode setting."),
        settings_path: settings_path.display().to_string(),
        state_path: state_path.display().to_string(),
        previous_local_mode: state.previous_local_mode,
        current_local_mode: get_nested_value(&document, PERMISSION_DEFAULT_MODE_PATH).cloned(),
    })
}

fn execute_structured_output(
    input: StructuredOutputInput,
) -> Result<StructuredOutputResult, String> {
    if input.0.is_empty() {
        return Err(String::from("structured output payload must not be empty"));
    }
    Ok(StructuredOutputResult {
        data: String::from("Structured output provided successfully"),
        structured_output: input.0,
    })
}

fn execute_repl(workspace_root: &Path, input: ReplInput) -> Result<ReplOutput, String> {
    if input.code.trim().is_empty() {
        return Err(String::from("code must not be empty"));
    }
    let language = input.language.trim().to_ascii_lowercase();
    if !matches!(
        language.as_str(),
        "python" | "py" | "javascript" | "js" | "node" | "bash" | "sh" | "shell"
    ) {
        return Err(format!("unsupported REPL language: {}", input.language));
    }
    let started = Instant::now();
    let output = crate::sandbox_exec::execute_code_in_workspace(
        &input.language,
        &input.code,
        input.timeout_ms,
        workspace_root,
    );
    if output.exit_code == 124 {
        return Err(format!(
            "REPL execution exceeded timeout of {} ms",
            input.timeout_ms.unwrap_or_default()
        ));
    }
    if output.exit_code != 0 {
        return Err(format!(
            "REPL execution failed with exit code {}: {}",
            output.exit_code,
            output.stderr.trim()
        ));
    }

    Ok(ReplOutput {
        language: input.language,
        stdout: output.stdout,
        stderr: output.stderr,
        exit_code: output.exit_code,
        duration_ms: started.elapsed().as_millis(),
    })
}

#[derive(Clone, Copy)]
enum ConfigScope {
    Global,
    Settings,
}

#[derive(Clone, Copy)]
struct ConfigSettingSpec {
    scope: ConfigScope,
    kind: ConfigKind,
    path: &'static [&'static str],
    options: Option<&'static [&'static str]>,
}

#[derive(Clone, Copy)]
enum ConfigKind {
    Boolean,
    String,
}

fn supported_config_setting(setting: &str) -> Option<ConfigSettingSpec> {
    Some(match setting {
        "theme" => ConfigSettingSpec {
            scope: ConfigScope::Global,
            kind: ConfigKind::String,
            path: &["theme"],
            options: None,
        },
        "editorMode" => ConfigSettingSpec {
            scope: ConfigScope::Global,
            kind: ConfigKind::String,
            path: &["editorMode"],
            options: Some(&["default", "vim", "emacs"]),
        },
        "verbose" => ConfigSettingSpec {
            scope: ConfigScope::Global,
            kind: ConfigKind::Boolean,
            path: &["verbose"],
            options: None,
        },
        "preferredNotifChannel" => ConfigSettingSpec {
            scope: ConfigScope::Global,
            kind: ConfigKind::String,
            path: &["preferredNotifChannel"],
            options: None,
        },
        "autoCompactEnabled" => ConfigSettingSpec {
            scope: ConfigScope::Global,
            kind: ConfigKind::Boolean,
            path: &["autoCompactEnabled"],
            options: None,
        },
        "autoMemoryEnabled" => ConfigSettingSpec {
            scope: ConfigScope::Settings,
            kind: ConfigKind::Boolean,
            path: &["autoMemoryEnabled"],
            options: None,
        },
        "autoDreamEnabled" => ConfigSettingSpec {
            scope: ConfigScope::Settings,
            kind: ConfigKind::Boolean,
            path: &["autoDreamEnabled"],
            options: None,
        },
        "fileCheckpointingEnabled" => ConfigSettingSpec {
            scope: ConfigScope::Global,
            kind: ConfigKind::Boolean,
            path: &["fileCheckpointingEnabled"],
            options: None,
        },
        "showTurnDuration" => ConfigSettingSpec {
            scope: ConfigScope::Global,
            kind: ConfigKind::Boolean,
            path: &["showTurnDuration"],
            options: None,
        },
        "terminalProgressBarEnabled" => ConfigSettingSpec {
            scope: ConfigScope::Global,
            kind: ConfigKind::Boolean,
            path: &["terminalProgressBarEnabled"],
            options: None,
        },
        "todoFeatureEnabled" => ConfigSettingSpec {
            scope: ConfigScope::Global,
            kind: ConfigKind::Boolean,
            path: &["todoFeatureEnabled"],
            options: None,
        },
        "model" => ConfigSettingSpec {
            scope: ConfigScope::Settings,
            kind: ConfigKind::String,
            path: &["model"],
            options: None,
        },
        "alwaysThinkingEnabled" => ConfigSettingSpec {
            scope: ConfigScope::Settings,
            kind: ConfigKind::Boolean,
            path: &["alwaysThinkingEnabled"],
            options: None,
        },
        "permissions.default_mode" => ConfigSettingSpec {
            scope: ConfigScope::Settings,
            kind: ConfigKind::String,
            path: &["permissions", "default_mode"],
            options: Some(&["read-only", "workspace-write", "danger-full-access"]),
        },
        "language" => ConfigSettingSpec {
            scope: ConfigScope::Settings,
            kind: ConfigKind::String,
            path: &["language"],
            options: None,
        },
        "teammateMode" => ConfigSettingSpec {
            scope: ConfigScope::Global,
            kind: ConfigKind::String,
            path: &["teammateMode"],
            options: Some(&["tmux", "in-process", "auto"]),
        },
        _ => return None,
    })
}

fn normalize_config_value(spec: ConfigSettingSpec, value: ConfigValue) -> Result<Value, String> {
    let normalized = match (spec.kind, value) {
        (ConfigKind::Boolean, ConfigValue::Bool(value)) => Value::Bool(value),
        (ConfigKind::Boolean, ConfigValue::String(value)) => {
            match value.trim().to_ascii_lowercase().as_str() {
                "true" => Value::Bool(true),
                "false" => Value::Bool(false),
                _ => return Err(String::from("setting requires true or false")),
            }
        }
        (ConfigKind::Boolean, ConfigValue::Number(_)) => {
            return Err(String::from("setting requires true or false"));
        }
        (ConfigKind::String, ConfigValue::String(value)) => Value::String(value),
        (ConfigKind::String, ConfigValue::Bool(value)) => Value::String(value.to_string()),
        (ConfigKind::String, ConfigValue::Number(value)) => json!(value),
    };

    if let Some(options) = spec.options {
        let Some(as_str) = normalized.as_str() else {
            return Err(String::from("setting requires a string value"));
        };
        if !options.iter().any(|option| option == &as_str) {
            return Err(format!(
                "Invalid value \"{as_str}\". Options: {}",
                options.join(", ")
            ));
        }
    }

    Ok(normalized)
}

fn config_file_for_scope(scope: ConfigScope, workspace_root: &Path) -> Result<PathBuf, String> {
    Ok(match scope {
        ConfigScope::Global => config_home_dir()?.join("config.yaml"),
        ConfigScope::Settings => workspace_root.join(".cowd").join("config.local.yaml"),
    })
}

fn config_home_dir() -> Result<PathBuf, String> {
    if let Ok(path) = std::env::var("COWD_CONFIG_HOME") {
        return Ok(PathBuf::from(path));
    }
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .map_err(|_| {
            String::from(
                "HOME is not set (on Windows, set USERPROFILE or HOME, \
                 or use CC_CONFIG_HOME to point directly at the config directory)",
            )
        })?;
    Ok(PathBuf::from(home).join(".cowd"))
}

fn read_json_object(path: &Path) -> Result<serde_json::Map<String, Value>, String> {
    match std::fs::read_to_string(path) {
        Ok(contents) => {
            if contents.trim().is_empty() {
                return Ok(serde_json::Map::new());
            }
            // Try JSON first (fast path), fall back to YAML parser
            // (YAML is a superset of JSON so both work)
            let val = serde_json::from_str::<Value>(&contents)
                .or_else(|_| serde_yaml::from_str::<Value>(&contents))
                .map_err(|error| {
                    format!("failed to parse config (tried JSON then YAML): {error}")
                })?;
            val.as_object()
                .cloned()
                .ok_or_else(|| String::from("config file must contain a JSON/YAML object"))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(serde_json::Map::new()),
        Err(error) => Err(error.to_string()),
    }
}

fn write_json_object(path: &Path, value: &serde_json::Map<String, Value>) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    // Write as well-formatted JSON (which is valid YAML)
    std::fs::write(
        path,
        serde_json::to_string_pretty(value).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
}

fn get_nested_value<'a>(
    value: &'a serde_json::Map<String, Value>,
    path: &[&str],
) -> Option<&'a Value> {
    let (first, rest) = path.split_first()?;
    let mut current = value.get(*first)?;
    for key in rest {
        current = current.as_object()?.get(*key)?;
    }
    Some(current)
}

fn set_nested_value(root: &mut serde_json::Map<String, Value>, path: &[&str], new_value: Value) {
    let Some((first, rest)) = path.split_first() else {
        return;
    };
    if rest.is_empty() {
        root.insert((*first).to_string(), new_value);
        return;
    }

    let entry = root
        .entry((*first).to_string())
        .or_insert_with(|| Value::Object(serde_json::Map::new()));
    if !entry.is_object() {
        *entry = Value::Object(serde_json::Map::new());
    }
    if let Some(map) = entry.as_object_mut() {
        set_nested_value(map, rest, new_value);
    }
}

fn remove_nested_value(root: &mut serde_json::Map<String, Value>, path: &[&str]) -> bool {
    let Some((first, rest)) = path.split_first() else {
        return false;
    };
    if rest.is_empty() {
        return root.remove(*first).is_some();
    }

    let mut should_remove_parent = false;
    let removed = root.get_mut(*first).is_some_and(|entry| {
        entry.as_object_mut().is_some_and(|map| {
            let removed = remove_nested_value(map, rest);
            should_remove_parent = removed && map.is_empty();
            removed
        })
    });

    if should_remove_parent {
        root.remove(*first);
    }

    removed
}

fn plan_mode_state_file(workspace_root: &Path) -> Result<PathBuf, String> {
    Ok(
        config_file_for_scope(ConfigScope::Settings, workspace_root)?
            .parent()
            .ok_or_else(|| String::from("config.local.yaml has no parent directory"))?
            .join("tool-state")
            .join("plan-mode.json"),
    )
}

fn read_plan_mode_state(path: &Path) -> Result<Option<PlanModeState>, String> {
    match std::fs::read_to_string(path) {
        Ok(contents) => {
            if contents.trim().is_empty() {
                return Ok(None);
            }
            serde_json::from_str(&contents)
                .map(Some)
                .map_err(|error| error.to_string())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

fn write_plan_mode_state(path: &Path, state: &PlanModeState) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    std::fs::write(
        path,
        serde_json::to_string_pretty(state).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
}

fn clear_plan_mode_state(path: &Path) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

fn iso8601_timestamp() -> String {
    if let Ok(output) = Command::new("date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()
    {
        if output.status.success() {
            return String::from_utf8_lossy(&output.stdout).trim().to_string();
        }
    }
    iso8601_now()
}

#[allow(clippy::needless_pass_by_value)]
fn execute_powershell(
    workspace_root: &Path,
    input: PowerShellInput,
) -> std::io::Result<BashCommandOutput> {
    if let Some(output) = workspace_test_branch_preflight(&input.command, None) {
        return Ok(output);
    }
    let shell = detect_powershell_shell()?;
    crate::bash::execute_bash_in_workspace(
        BashCommandInput {
            command: format!(
                "exec {} -NoProfile -NonInteractive -Command {}",
                shell_quote(&shell.display().to_string()),
                shell_quote(&input.command)
            ),
            cwd: None,
            timeout_ms: input.timeout_ms,
            description: input.description,
            run_in_background: input.run_in_background,
            dangerously_disable_sandbox: Some(false),
            isolate_network: None,
            workspace_access: Some(crate::bash::BashWorkspaceAccess::ReadWrite),
            allowed_mounts: None,
            env: None,
        },
        workspace_root,
    )
}

fn detect_powershell_shell() -> std::io::Result<PathBuf> {
    if let Some(path) = find_command("pwsh") {
        Ok(path)
    } else if let Some(path) = find_command("powershell") {
        Ok(path)
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "PowerShell executable not found (expected `pwsh` or `powershell` in PATH)",
        ))
    }
}

fn find_command(name: &str) -> Option<PathBuf> {
    // Safety: validate input to prevent path traversal and injection
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return None;
    }
    // Search PATH directories for the executable
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths).find_map(|dir| {
            let full_path = dir.join(name);
            if !full_path.is_file() {
                return None;
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::metadata(&full_path)
                    .map(|m| m.permissions().mode() & 0o111 != 0)
                    .unwrap_or(false)
                    .then(|| full_path.canonicalize().unwrap_or(full_path))
            }
            #[cfg(not(unix))]
            {
                Some(full_path.canonicalize().unwrap_or(full_path))
            }
        })
    })
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn resolve_cell_index(
    cells: &[serde_json::Value],
    cell_id: Option<&str>,
    edit_mode: NotebookEditMode,
) -> Result<usize, String> {
    if cells.is_empty()
        && matches!(
            edit_mode,
            NotebookEditMode::Replace | NotebookEditMode::Delete
        )
    {
        return Err(String::from("Notebook has no cells to edit"));
    }
    if let Some(cell_id) = cell_id {
        cells
            .iter()
            .position(|cell| cell.get("id").and_then(serde_json::Value::as_str) == Some(cell_id))
            .ok_or_else(|| format!("Cell id not found: {cell_id}"))
    } else {
        Ok(cells.len().saturating_sub(1))
    }
}

fn source_lines(source: &str) -> Vec<serde_json::Value> {
    if source.is_empty() {
        return vec![serde_json::Value::String(String::new())];
    }
    source
        .split_inclusive('\n')
        .map(|line| serde_json::Value::String(line.to_string()))
        .collect()
}

fn format_notebook_edit_mode(mode: NotebookEditMode) -> String {
    match mode {
        NotebookEditMode::Replace => String::from("replace"),
        NotebookEditMode::Insert => String::from("insert"),
        NotebookEditMode::Delete => String::from("delete"),
    }
}

fn make_cell_id(index: usize) -> String {
    format!("cell-{}", index + 1)
}
