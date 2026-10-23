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
    edit_file, glob_search_with_options, grep_search, read_file, write_file, GrepSearchInput,
    SearchOptions,
};
use crate::lane_events::{LaneEvent, LaneEventName, LaneEventStatus, LaneFailureClass};
use crate::lane_policy::{iso8601_now, LaneContext};
use crate::mutation_plan::{
    apply_mutations, preview_mutations, MutationApplyInput, MutationPreviewInput,
};
use crate::path_policy::WorkspacePathPolicy;
use crate::search::{execute_web_search, WebSearchInput};
use crate::stale_branch::{check_freshness, BranchFreshness};
use crate::ToolHostLease;

#[path = "dispatch.rs"]
mod dispatch;
pub(crate) use dispatch::execute_with_lease;
#[path = "builtin.rs"]
mod builtin;
use builtin::*;
#[path = "orchestrate.rs"]
pub mod orchestrate;
pub(crate) use orchestrate::run_orchestrate;

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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BackgroundTaskStatusInput {
    job_id: String,
    #[serde(default)]
    tail: Option<usize>,
}

fn run_background_task_status(lease: &ToolHostLease, input: &Value) -> Result<String, String> {
    let parsed: BackgroundTaskStatusInput = from_value(input)?;
    crate::background_jobs::job_status(lease.workspace_root(), &parsed.job_id, parsed.tail)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BackgroundTaskKillInput {
    job_id: String,
    /// Accepted and ignored: `tail` belongs to `status`, but the merged tool
    /// shares one schema, so rejecting it here would break a valid kill call.
    #[serde(default)]
    #[allow(dead_code)]
    tail: Option<usize>,
}

fn run_background_task_kill(lease: &ToolHostLease, input: &Value) -> Result<String, String> {
    let parsed: BackgroundTaskKillInput = from_value(input)?;
    crate::background_jobs::kill_job(lease.workspace_root(), &parsed.job_id)
}

#[derive(Debug, Deserialize)]
struct GrepSearchLanguageInput {
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

fn run_grep_search_language(
    lease: &ToolHostLease,
    input: GrepSearchLanguageInput,
) -> Result<String, String> {
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
        "kind": "grep_search",
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
        // A preflight refusal never spawned a command, so there is no process
        // closure to prove.
        process_closure: None,
    }
}

#[allow(clippy::needless_pass_by_value)]
fn run_read_file(lease: &ToolHostLease, input: ReadFileInput) -> Result<String, String> {
    let resolved = lease
        .path_policy()
        .resolve(&input.path)
        .map_err(io_to_string)?;
    let operation = || {
        read_file(
            lease.path_policy(),
            &input.path,
            input.offset,
            input.limit,
            input.complete,
        )
        .map_err(io_to_string)
    };
    let Some(authority) = lease.read_cache_authority_digest() else {
        return to_pretty_json(operation()?);
    };
    // A windowed read fingerprints at most 16 KiB; an explicit whole-file
    // (`complete`) read keeps the full content hash so an EOF receipt stays
    // bound to the exact bytes.
    let fingerprint = file_fingerprint(&resolved, input.complete);
    let scope = file_cache_scope(&resolved);
    let cache_input = serde_json::to_string(
        &json!({"input":input,"fingerprint":fingerprint,"authority":authority}),
    )
    .map_err(|e| e.to_string())?;
    lease.cache().with_read_flight(
        lease.workspace_id(),
        &scope,
        "read_file",
        &cache_input,
        lease.schema_revision(),
        || {
            if let Some(cached) = lease.cache().get(
                lease.workspace_id(),
                &scope,
                "read_file",
                &cache_input,
                lease.schema_revision(),
            ) {
                return Ok(cached);
            }
            let token = lease.cache().epoch_token(&scope);
            let output = to_pretty_json(operation()?)?;
            if file_fingerprint(&resolved, input.complete) == fingerprint {
                if let Some(token) = token {
                    lease.cache().put_if_current(
                        lease.workspace_id(),
                        &scope,
                        "read_file",
                        &cache_input,
                        lease.schema_revision(),
                        &output,
                        token,
                    );
                }
            }
            Ok(output)
        },
    )
}

#[allow(clippy::needless_pass_by_value)]
/// Maximum items one flat batch tool call may enqueue in a single request.
///
/// Matches the orchestration engine's own per-step item limit, so the two batch
/// surfaces behave the same, and stops one call from enqueuing unbounded work
/// while only `max_concurrency` of it can run at a time.
const MAX_BATCH_ITEMS: usize = 200;

fn require_batch_bound(tool: &str, items: usize) -> Result<(), String> {
    if items > MAX_BATCH_ITEMS {
        return Err(format!(
            "{tool} accepts at most {MAX_BATCH_ITEMS} items per call; this call has {items}. \
             Split it into several calls, or use tool_orchestrate whose map step fans out the same way."
        ));
    }
    Ok(())
}

#[allow(clippy::needless_pass_by_value)]
fn run_write_file(lease: &ToolHostLease, input: WriteFileInput) -> Result<String, String> {
    let scope = file_cache_scope(
        &lease
            .path_policy()
            .resolve(&input.path)
            .map_err(io_to_string)?,
    );
    create_auto_checkpoint(lease, "edit", &[input.path.clone()])?;
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
    create_auto_checkpoint(lease, "edit", &[input.path.clone()])?;
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
    let checkpoint_paths = input
        .edits
        .iter()
        .map(|edit| edit.path.clone())
        .collect::<Vec<_>>();
    create_auto_checkpoint(lease, "edit", &checkpoint_paths)?;
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

fn create_auto_checkpoint(
    lease: &ToolHostLease,
    tool_name: &str,
    paths: &[String],
) -> Result<(), String> {
    if std::env::var("COWD_AUTO_CHECKPOINT").ok().as_deref() != Some("1") {
        return Ok(());
    }
    if paths.is_empty() {
        return Err("automatic checkpoint requires at least one exact file target".into());
    }
    let mut targets = paths.to_vec();
    targets.sort();
    targets.dedup();
    for target in &targets {
        if target.trim() != target || target.is_empty() || target.contains('\\') {
            return Err(
                "automatic checkpoint requires canonical workspace-relative file targets".into(),
            );
        }
        let mut relative = PathBuf::new();
        for component in Path::new(target).components() {
            match component {
                std::path::Component::Normal(part) if part != ".cowd" => relative.push(part),
                _ => {
                    return Err(
                        "automatic checkpoint target is not a bounded workspace file".into(),
                    )
                }
            }
        }
        if relative.to_str() != Some(target.as_str()) {
            return Err("automatic checkpoint target has a noncanonical spelling".into());
        }
        lease.path_policy().resolve(target).map_err(io_to_string)?;
        let mut cursor = lease.workspace_root().to_path_buf();
        for component in relative.components() {
            cursor.push(component.as_os_str());
            match std::fs::symlink_metadata(&cursor) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err("automatic checkpoint target crosses a symlink".into());
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Err(error) => return Err(io_to_string(error)),
            }
        }
        match std::fs::symlink_metadata(lease.workspace_root().join(&relative)) {
            Ok(metadata) if !metadata.is_file() => {
                return Err("automatic checkpoint target must be a regular file".into());
            }
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                return Err(io_to_string(error))
            }
            _ => {}
        }
    }
    checkpoint_create_in(
        lease.workspace_root(),
        CheckpointCreateInput {
            label: Some(format!("auto-before-{tool_name}")),
            paths: targets,
        },
    )
    .map(|_| ())
    .map_err(io_to_string)
}

fn run_checkpoint_list(lease: &ToolHostLease) -> Result<String, String> {
    to_pretty_json(checkpoint_list_in(lease.workspace_root()).map_err(io_to_string)?)
}

#[cfg(test)]
mod auto_checkpoint_contract_tests {
    use super::*;
    use std::collections::BTreeSet;

    struct StdOwnedWorkspace {
        path: PathBuf,
    }

    impl StdOwnedWorkspace {
        fn new() -> std::io::Result<Self> {
            static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(std::io::Error::other)?
                .as_nanos();
            let sequence = SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "cowd-executor-auto-contract-{}-{timestamp}-{sequence}",
                std::process::id()
            ));
            std::fs::create_dir(&path)?;
            Ok(Self { path })
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for StdOwnedWorkspace {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    struct CheckpointEnvironment(Option<std::ffi::OsString>);

    impl Drop for CheckpointEnvironment {
        fn drop(&mut self) {
            match &self.0 {
                Some(value) => std::env::set_var("COWD_AUTO_CHECKPOINT", value),
                None => std::env::remove_var("COWD_AUTO_CHECKPOINT"),
            }
        }
    }

    fn checkpoint_ids(root: &Path) -> BTreeSet<String> {
        checkpoint_list_in(root)
            .unwrap()
            .checkpoints
            .into_iter()
            .map(|checkpoint| checkpoint.id)
            .collect()
    }

    fn new_checkpoint(
        root: &Path,
        before: &BTreeSet<String>,
        targets: &[&str],
    ) -> CheckpointSummaryForTest {
        let added = checkpoint_ids(root)
            .difference(before)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(added.len(), 1);
        let directory = root.join(".cowd/checkpoints").join(&added[0]);
        let manifest: Value =
            serde_json::from_slice(&std::fs::read(directory.join(".manifest.json")).unwrap())
                .unwrap();
        assert_eq!(manifest["scopes"], json!(targets));
        assert!(!directory.join("sentinel.env").exists());
        assert!(!directory.join("src/unrelated.txt").exists());
        CheckpointSummaryForTest {
            id: added[0].clone(),
            directory,
        }
    }

    struct CheckpointSummaryForTest {
        id: String,
        directory: PathBuf,
    }

    #[test]
    fn r1_auto_checkpoint_exact_targets_preserve_restore_and_unknown_boundaries() {
        const CHILD_ENV: &str = "COWD_R1_AUTO_CHECKPOINT_CONTRACT_CHILD";
        const CHILD_VALUE: &str = "exact-matrix-v1";
        const COMPLETION: &str = "COWD_R1_AUTO_CHECKPOINT_CONTRACT_MATRIX_COMPLETE";
        if std::env::var_os(CHILD_ENV).as_deref() != Some(std::ffi::OsStr::new(CHILD_VALUE)) {
            let test_name = "executor::auto_checkpoint_contract_tests::r1_auto_checkpoint_exact_targets_preserve_restore_and_unknown_boundaries";
            let output = Command::new(std::env::current_exe().expect("test executable"))
                .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
                .env(CHILD_ENV, CHILD_VALUE)
                .output()
                .expect("spawn isolated checkpoint contract");
            {
                use std::io::Write;
                let mut stderr = std::io::stderr().lock();
                let stdout_result = stderr.write_all(&output.stdout);
                let stderr_result = stderr.write_all(&output.stderr);
                if stdout_result.is_err() || stderr_result.is_err() {
                    eprintln!("child diagnostic write failed: stdout={stdout_result:?}, stderr={stderr_result:?}");
                }
            }
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                output.status.success()
                    && stdout.lines().any(|line| line == "running 1 test")
                    && stdout
                        .lines()
                        .any(|line| line.starts_with(&format!("test {test_name} ...")))
                    && stdout.lines().any(|line| line.trim() == COMPLETION)
                    && stdout.lines().any(|line| {
                        line.starts_with("test result: ok. 1 passed; 0 failed; 0 ignored;")
                    }),
                "isolated checkpoint contract failed: {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
                output.status
            );
            return;
        }

        let _lock = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _environment = CheckpointEnvironment(std::env::var_os("COWD_AUTO_CHECKPOINT"));
        std::env::set_var("COWD_AUTO_CHECKPOINT", "1");
        let workspace = StdOwnedWorkspace::new().unwrap();
        let root = workspace.path();
        std::fs::create_dir(root.join("src")).unwrap();
        std::fs::write(root.join("src/a"), "alpha\n").unwrap();
        std::fs::write(root.join("src/b"), "bravo\n").unwrap();
        std::fs::write(root.join("src/unrelated.txt"), "unrelated\n").unwrap();
        std::fs::write(root.join("sentinel.env"), "fixture-secret-not-to-be-read\n").unwrap();
        let lease = crate::ToolHost::builtin("scoped-auto-contract", root).pin_snapshot();

        let purpose_input = json!({"action":"write","path":"src/a","content":"off\n"});
        let checkpoint_on_resolution = lease.describe_effect_resolution("edit", &purpose_input);
        assert!(checkpoint_on_resolution
            .scope_purposes
            .iter()
            .any(|entry| entry.purpose
                == harness_contract::goal::ProofEffectPurpose::AutoCheckpointMaintenance));

        for corruption in ["missing", "json", "directory", "scopes", "unknown"] {
            let before = checkpoint_ids(root);
            execute_with_lease(
                &lease,
                "edit",
                &json!({"action":"write","path":"src/a","content":"fault current\n"}),
            )
            .unwrap();
            let checkpoint = new_checkpoint(root, &before, &["src/a"]);
            let path = checkpoint.directory.join(".manifest.json");
            let valid = std::fs::read(&path).unwrap();
            let mut value: Value = serde_json::from_slice(&valid).unwrap();
            match corruption {
                "missing" => std::fs::remove_file(&path).unwrap(),
                "json" => std::fs::write(&path, "{").unwrap(),
                "directory" => {
                    std::fs::remove_file(&path).unwrap();
                    std::fs::create_dir(&path).unwrap();
                }
                "scopes" => {
                    value.as_object_mut().unwrap().remove("scopes");
                    std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
                }
                "unknown" => {
                    value["unknown"] = json!(true);
                    std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
                }
                _ => unreachable!(),
            }
            lease.cache().put(
                lease.workspace_id(),
                "file:src/a",
                "read_file",
                "fault-cache",
                lease.schema_revision(),
                "preserve cached read",
            );
            let epoch = lease.cache().epoch_token("file:src/a");
            let backups = root.join(".cowd/restore-backups");
            let before_backups = if backups.exists() {
                std::fs::read_dir(&backups)
                    .unwrap()
                    .map(|entry| entry.unwrap().file_name())
                    .collect::<BTreeSet<_>>()
            } else {
                BTreeSet::new()
            };
            for action in ["diff", "restore"] {
                let error = execute_with_lease(
                    &lease,
                    "checkpoint",
                    &json!({"action":action,"id":checkpoint.id}),
                )
                .unwrap_err();
                assert!(!error.is_empty(), "{corruption}/{action}");
                match corruption {
                    "directory" => assert!(error.to_lowercase().contains("directory"), "{error}"),
                    "missing" => assert!(
                        error.contains("No such file") || error.contains("not found"),
                        "{error}"
                    ),
                    "scopes" => assert!(
                        error.contains("missing field") && error.contains("scopes"),
                        "{error}"
                    ),
                    "unknown" => assert!(error.contains("unknown field"), "{error}"),
                    "json" => assert!(
                        error.contains("EOF") || error.contains("expected"),
                        "{error}"
                    ),
                    _ => unreachable!(),
                }
                assert_eq!(lease.cache().epoch_token("file:src/a"), epoch);
                assert_eq!(
                    lease
                        .cache()
                        .get(
                            lease.workspace_id(),
                            "file:src/a",
                            "read_file",
                            "fault-cache",
                            lease.schema_revision()
                        )
                        .as_deref(),
                    Some("preserve cached read")
                );
                assert_eq!(
                    std::fs::read_to_string(root.join("src/a")).unwrap(),
                    "fault current\n"
                );
                assert_eq!(
                    std::fs::read_to_string(root.join("src/unrelated.txt")).unwrap(),
                    "unrelated\n"
                );
                assert_eq!(
                    std::fs::read_to_string(root.join("sentinel.env")).unwrap(),
                    "fixture-secret-not-to-be-read\n"
                );
                assert_eq!(
                    std::fs::read_to_string(checkpoint.directory.join("src/a")).unwrap(),
                    "alpha\n"
                );
                let after_backups = if backups.exists() {
                    std::fs::read_dir(&backups)
                        .unwrap()
                        .map(|entry| entry.unwrap().file_name())
                        .collect::<BTreeSet<_>>()
                } else {
                    BTreeSet::new()
                };
                assert_eq!(before_backups, after_backups);
            }
            if corruption == "directory" {
                std::fs::remove_dir(&path).unwrap();
            }
            std::fs::write(&path, valid).unwrap();
            execute_with_lease(
                &lease,
                "checkpoint",
                &json!({"action":"restore","id":checkpoint.id}),
            )
            .unwrap();
            assert_ne!(lease.cache().epoch_token("file:src/a"), epoch);
            assert_eq!(
                std::fs::read_to_string(root.join("src/a")).unwrap(),
                "alpha\n"
            );
        }
        let before = checkpoint_ids(root);
        execute_with_lease(
            &lease,
            "edit",
            &json!({"action":"write","path":"src/a","content":"changed\n"}),
        )
        .unwrap();
        let checkpoint = new_checkpoint(root, &before, &["src/a"]);
        assert_eq!(
            std::fs::read_to_string(checkpoint.directory.join("src/a")).unwrap(),
            "alpha\n"
        );
        assert_eq!(
            std::fs::read_to_string(root.join("src/a")).unwrap(),
            "changed\n"
        );
        checkpoint_restore_in(root, CheckpointRestoreInput { id: checkpoint.id }).unwrap();
        assert_eq!(
            std::fs::read_to_string(root.join("src/a")).unwrap(),
            "alpha\n"
        );

        let before = checkpoint_ids(root);
        execute_with_lease(
            &lease,
            "edit",
            &json!({"action":"replace","path":"src/a","old_string":"alpha","new_string":"omega"}),
        )
        .unwrap();
        let checkpoint = new_checkpoint(root, &before, &["src/a"]);
        assert_eq!(
            std::fs::read_to_string(root.join("src/a")).unwrap(),
            "omega\n"
        );
        checkpoint_restore_in(root, CheckpointRestoreInput { id: checkpoint.id }).unwrap();

        let before = checkpoint_ids(root);
        execute_with_lease(
            &lease,
            "edit",
            &json!({"action":"patch","edits":[
                {"path":"src/b","old_string":"bravo","new_string":"second"},
                {"path":"src/a","old_string":"alpha","new_string":"first"}
            ]}),
        )
        .unwrap();
        let checkpoint = new_checkpoint(root, &before, &["src/a", "src/b"]);
        assert_eq!(
            std::fs::read_to_string(checkpoint.directory.join("src/a")).unwrap(),
            "alpha\n"
        );
        assert_eq!(
            std::fs::read_to_string(checkpoint.directory.join("src/b")).unwrap(),
            "bravo\n"
        );
        assert_eq!(
            std::fs::read_to_string(root.join("src/a")).unwrap(),
            "first\n"
        );
        assert_eq!(
            std::fs::read_to_string(root.join("src/b")).unwrap(),
            "second\n"
        );
        checkpoint_restore_in(root, CheckpointRestoreInput { id: checkpoint.id }).unwrap();

        let before = checkpoint_ids(root);
        execute_with_lease(
            &lease,
            "edit",
            &json!({"action":"write","path":"src/new","content":"new\n"}),
        )
        .unwrap();
        let checkpoint = new_checkpoint(root, &before, &["src/new"]);
        assert!(!checkpoint.directory.join("src/new").exists());
        checkpoint_restore_in(root, CheckpointRestoreInput { id: checkpoint.id }).unwrap();
        assert!(!root.join("src/new").exists());

        for target in [
            "",
            " ",
            ".",
            "./src/a",
            "src//a",
            "src/../src/a",
            "../escape",
            " src/a ",
            "src\\a",
            ".cowd/checkpoints/escape",
            "src",
        ] {
            let before = checkpoint_ids(root);
            assert!(
                execute_with_lease(
                    &lease,
                    "edit",
                    &json!({"action":"write","path":target,"content":"forbidden"})
                )
                .is_err(),
                "{target}"
            );
            assert_eq!(checkpoint_ids(root), before, "{target}");
            assert_eq!(
                std::fs::read_to_string(root.join("src/a")).unwrap(),
                "alpha\n"
            );
        }
        let before = checkpoint_ids(root);
        assert!(execute_with_lease(
            &lease,
            "edit",
            &json!({"action":"write","path":root.join("src/a"),"content":"absolute"})
        )
        .is_err());
        assert!(execute_with_lease(
            &lease,
            "edit",
            &json!({"action":"write","content":"missing target"})
        )
        .is_err());
        assert!(execute_with_lease(&lease, "edit", &json!({"action":"patch","edits":[]})).is_err());
        assert!(execute_with_lease(
            &lease,
            "edit",
            &json!({"action":"patch","edits":[
                {"path":"src/a","old_string":"alpha","new_string":"forbidden"},
                {"path":"../escape","old_string":"outside","new_string":"forbidden"}
            ]})
        )
        .is_err());
        assert_eq!(checkpoint_ids(root), before);
        assert_eq!(
            std::fs::read_to_string(root.join("src/a")).unwrap(),
            "alpha\n"
        );
        assert_eq!(
            std::fs::read_to_string(root.join("src/b")).unwrap(),
            "bravo\n"
        );

        let before = checkpoint_ids(root);
        assert!(execute_with_lease(
            &lease,
            "edit",
            &json!({"action":"patch","edits":[
            {"path":"src/a","old_string":"alpha","new_string":"forbidden"},
            {"path":"src/b","old_string":"bravo","new_string":"forbidden"}
        ],"expected_hashes":{"src/b":"sha256:deliberately-stale"}})
        )
        .is_err());
        new_checkpoint(root, &before, &["src/a", "src/b"]);
        assert_eq!(
            std::fs::read_to_string(root.join("src/a")).unwrap(),
            "alpha\n"
        );
        assert_eq!(
            std::fs::read_to_string(root.join("src/b")).unwrap(),
            "bravo\n"
        );

        #[cfg(unix)]
        {
            let outside = StdOwnedWorkspace::new().unwrap();
            std::fs::write(outside.path().join("a"), "outside\n").unwrap();
            std::os::unix::fs::symlink(outside.path(), root.join("outside-link")).unwrap();
            std::os::unix::fs::symlink(root.join("src/a"), root.join("inside-link")).unwrap();
            std::os::unix::fs::symlink(root.join("src"), root.join("inside-directory-link"))
                .unwrap();
            for target in ["outside-link/a", "inside-link", "inside-directory-link/a"] {
                let before = checkpoint_ids(root);
                assert!(execute_with_lease(
                    &lease,
                    "edit",
                    &json!({"action":"write","path":target,"content":"forbidden"})
                )
                .is_err());
                assert_eq!(checkpoint_ids(root), before);
            }
            assert_eq!(
                std::fs::read_to_string(outside.path().join("a")).unwrap(),
                "outside\n"
            );
            for target in ["outside-link", "inside-link", "inside-directory-link"] {
                std::fs::remove_file(root.join(target)).unwrap();
            }
        }

        let failed_workspace = StdOwnedWorkspace::new().unwrap();
        std::fs::write(failed_workspace.path().join("a"), "preimage").unwrap();
        std::fs::create_dir(failed_workspace.path().join(".cowd")).unwrap();
        std::fs::write(
            failed_workspace.path().join(".cowd/checkpoints"),
            "storage blocker",
        )
        .unwrap();
        let failed_lease =
            crate::ToolHost::builtin("auto-storage-failure", failed_workspace.path())
                .pin_snapshot();
        assert!(execute_with_lease(
            &failed_lease,
            "edit",
            &json!({"action":"write","path":"a","content":"must not write"})
        )
        .is_err());
        assert_eq!(
            std::fs::read_to_string(failed_workspace.path().join("a")).unwrap(),
            "preimage"
        );

        let failed_input = json!({"action":"write","path":"a","content":"must not write"});
        let failed_resolution = failed_lease.describe_effect_resolution("edit", &failed_input);
        assert!(failed_resolution
            .scope_purposes
            .iter()
            .any(|entry| entry.purpose
                == harness_contract::goal::ProofEffectPurpose::AutoCheckpointMaintenance));
        std::fs::remove_file(failed_workspace.path().join(".cowd/checkpoints")).unwrap();
        let before_retry = checkpoint_ids(failed_workspace.path());
        execute_with_lease(&failed_lease, "edit", &failed_input).unwrap();
        let recovered_checkpoint = new_checkpoint(failed_workspace.path(), &before_retry, &["a"]);
        assert_eq!(
            std::fs::read_to_string(failed_workspace.path().join("a")).unwrap(),
            "must not write"
        );
        checkpoint_restore_in(
            failed_workspace.path(),
            CheckpointRestoreInput {
                id: recovered_checkpoint.id,
            },
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(failed_workspace.path().join("a")).unwrap(),
            "preimage"
        );
        assert_eq!(
            failed_resolution,
            failed_lease.describe_effect_resolution("edit", &failed_input)
        );

        std::env::remove_var("COWD_AUTO_CHECKPOINT");
        let before = checkpoint_ids(root);
        assert_eq!(
            checkpoint_on_resolution,
            lease.describe_effect_resolution("edit", &purpose_input)
        );
        execute_with_lease(
            &lease,
            "edit",
            &json!({"action":"write","path":"src/a","content":"off\n"}),
        )
        .unwrap();
        assert_eq!(checkpoint_ids(root), before);
        let explicit_resolution = lease.describe_effect_resolution(
            "checkpoint",
            &json!({"action":"create","paths":["src/a"]}),
        );
        assert!(!explicit_resolution
            .scope_purposes
            .iter()
            .any(|entry| entry.purpose
                == harness_contract::goal::ProofEffectPurpose::AutoCheckpointMaintenance));
        let explicit = checkpoint_create_in(
            root,
            CheckpointCreateInput {
                label: Some("explicit-full".into()),
                paths: Vec::new(),
            },
        )
        .unwrap();
        assert!(Path::new(&explicit.path)
            .join("src/unrelated.txt")
            .is_file());
        assert_eq!(
            std::fs::read_to_string(root.join("src/unrelated.txt")).unwrap(),
            "unrelated\n"
        );
        assert_eq!(
            std::fs::read_to_string(root.join("sentinel.env")).unwrap(),
            "fixture-secret-not-to-be-read\n"
        );
        println!("\n{COMPLETION}");
    }
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
    // Live filesystem pages carry their own source cursor. The old scope
    // fingerprint excludes fixed directory names and cannot certify these
    // user-overridable search scopes.
    to_pretty_json(
        glob_search_with_options(
            lease.path_policy(),
            &input.pattern,
            Some(resolved_path),
            input.cursor.as_deref(),
            &input.search_options,
        )
        .map_err(io_to_string)?,
    )
}

#[allow(clippy::needless_pass_by_value)]
fn run_grep_search(lease: &ToolHostLease, input: GrepSearchInput) -> Result<String, String> {
    to_pretty_json(grep_search(lease.path_policy(), &input).map_err(io_to_string)?)
}

#[derive(Debug, Deserialize)]
struct GlobSearchManyInput {
    patterns: Vec<GlobSearchInputValue>,
    #[serde(default)]
    max_concurrency: Option<usize>,
}

/// Several independent glob patterns in one call — the symmetric counterpart
/// of `grep_search.searches`, restoring the batch `glob_many` capability that
/// the family merge otherwise moved entirely into `tool_orchestrate`.
#[allow(clippy::needless_pass_by_value)]
fn run_glob_search_many(
    lease: &ToolHostLease,
    input: GlobSearchManyInput,
) -> Result<String, String> {
    require_batch_bound("glob_search", input.patterns.len())?;
    let results = run_ordered_batch(input.patterns, input.max_concurrency, |item| {
        let output = run_glob_search(lease, item)?;
        serde_json::from_str(&output).or(Ok(Value::String(output)))
    });
    to_pretty_json(batch_output("glob_search", results))
}

#[allow(clippy::needless_pass_by_value)]
fn run_grep_search_batch(
    lease: &ToolHostLease,
    input: GrepSearchBatchInput,
) -> Result<String, String> {
    require_batch_bound("grep_search", input.searches.len())?;
    let results = run_ordered_batch(input.searches, input.max_concurrency, |item| {
        let output = run_grep_search(lease, item)?;
        serde_json::from_str(&output).or(Ok(Value::String(output)))
    });
    to_pretty_json(batch_output("grep_search", results))
}

#[path = "workspace_snapshot.rs"]
mod workspace_snapshot;
use workspace_snapshot::{run_workspace_snapshot, WorkspaceSnapshotInput};

fn file_cache_scope(path: &Path) -> String {
    format!("file:{}", path.to_string_lossy())
}

/// Content fingerprint for the read cache.
///
/// A cache *hit* must not cost a full file read. The previous implementation
/// hashed every byte before the flight, so serving a hit still paid
/// `O(file size)` - for a 30 KiB source file that is 30 KiB of I/O per hit,
/// across every file of a multi-file batch.
///
/// `exact = false` (the common windowed read) now fingerprints
/// `len + mtime_nanos + sha256(head 8 KiB) + sha256(tail 8 KiB)`: at most
/// 16 KiB of reads regardless of file size. A same-length edit that lands in
/// the middle of a large file is still caught by the head+tail hashes when it
/// touches either end, and by `mtime_nanos` otherwise (cowd's own writes also
/// invalidate the cache scope explicitly, so internal edits never go stale).
///
/// `exact = true` (an explicit whole-file / EOF read) keeps the full content
/// hash: an EOF receipt must be bound to the exact bytes it claims to have
/// read end to end.
pub(super) fn file_fingerprint(resolved: &Path, exact: bool) -> String {
    let Ok(metadata) = std::fs::metadata(resolved) else {
        return format!("missing:{}", resolved.to_string_lossy());
    };
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let head = format!(
        "file:{}:{}:{}",
        resolved.to_string_lossy(),
        metadata.len(),
        modified
    );
    if exact {
        return head + &format!(":{}", full_content_hash(resolved));
    }
    format!(
        "{}:{}",
        head,
        bounded_content_hash(resolved, metadata.len())
    )
}

pub(super) const FINGERPRINT_EDGE_BYTES: usize = 8 * 1024;

/// Hash the first and last `FINGERPRINT_EDGE_BYTES` of a file.
pub(super) fn bounded_content_hash(resolved: &Path, len: u64) -> String {
    use std::io::{Read, Seek, SeekFrom};

    let Ok(mut file) = std::fs::File::open(resolved) else {
        return String::from("missing");
    };
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    let mut buffer = [0_u8; FINGERPRINT_EDGE_BYTES];
    // head
    match file.read(&mut buffer) {
        Ok(0) => return String::from("empty"),
        Ok(read) => buffer[..read].hash(&mut hasher),
        Err(_) => return String::from("unreadable"),
    };
    // tail (only when the file is larger than one edge, so small files stay exact)
    let len = usize::try_from(len).unwrap_or(usize::MAX);
    if len > FINGERPRINT_EDGE_BYTES {
        let tail_start = len.saturating_sub(FINGERPRINT_EDGE_BYTES) as u64;
        if file.seek(SeekFrom::Start(tail_start)).is_ok() {
            match file.read(&mut buffer) {
                Ok(read) => buffer[..read].hash(&mut hasher),
                Err(_) => return String::from("unreadable"),
            };
        }
    }
    format!("{:016x}", hasher.finish())
}

/// Hash every byte of a file. Reserved for explicit whole-file / EOF reads,
/// where the receipt must be bound to the exact content.
pub(super) fn full_content_hash(resolved: &Path) -> String {
    let Ok(mut file) = std::fs::File::open(resolved) else {
        return String::from("missing");
    };
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

/// Tools `tool_orchestrate` may dispatch.
///
/// PLAN E2: the vocabulary is the canonical built-in catalog, not a hand-copied
/// subset. The previous allow-list named 17 tools, so three quarters of the
/// inventory (`ast_grep_search`, `execute_code`, `lsp`, the skill/checkpoint
/// families, ...) could not be composed at all - the model was pushed back into
/// one-call-per-round.
///
/// Safety is unchanged: `tool_orchestrate` itself is excluded (no nesting), and
/// the runtime/gateway additionally checks every sub-call against the caller's
/// own allow-list before delegation
/// (`agent::in_process::tool_turn::reject_orchestrated_tools_outside_allow_list`),
/// so widening this set cannot grant a tool the caller was not already
/// authorized to invoke directly.
pub(crate) fn is_allowed_orchestration_tool(name: &str) -> bool {
    if name == "tool_orchestrate" {
        return false;
    }
    crate::mvp_tool_specs().iter().any(|spec| spec.name == name)
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
pub(crate) struct BatchToolItemOutput {
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

pub(crate) fn run_ordered_batch<T, F>(
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
fn run_web_fetch(lease: &ToolHostLease, input: WebFetchInput) -> Result<String, String> {
    to_pretty_json(execute_web_fetch(lease, &input)?)
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
        "activation": "staged_only_pending_human_approval",
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
    #[serde(default)]
    cursor: Option<String>,
    #[serde(flatten)]
    search_options: SearchOptions,
}

#[derive(Debug, Deserialize)]
struct GrepSearchBatchInput {
    searches: Vec<GrepSearchInput>,
    max_concurrency: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize)]
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
    #[serde(default)]
    status: BriefStatus,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum BriefStatus {
    #[default]
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

fn execute_web_fetch(lease: &ToolHostLease, input: &WebFetchInput) -> Result<WebFetchOutput, String> {
    let operation = lease.begin_local_network_operation(&serde_json::to_value(input).map_err(|error| error.to_string())?, &input.url)?;
    let started = Instant::now();
    let policy = crate::network_policy::NetworkDomainPolicy::from_env();
    let policy_receipt = policy.merge_call_filters(
        input.allowed_domains.as_deref(),
        input.blocked_domains.as_deref(),
    );
    if policy_receipt.denied || policy_receipt.requires_approval {
        lease.finish_local_network_operation(operation, harness_contract::context::LocalNetworkTerminal::NotStarted {
            reason_code: "network_policy_before_send".into(),
        })?;
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
    let request_url = normalize_fetch_url(&input.url)?;
    let url_receipt = policy.enforce_url_with_filters(
        &request_url, input.allowed_domains.as_deref(), input.blocked_domains.as_deref(),
    )?;
    if url_receipt.denied || url_receipt.requires_approval {
        lease.finish_local_network_operation(operation, harness_contract::context::LocalNetworkTerminal::NotStarted {
            reason_code: "network_url_policy_before_send".into(),
        })?;
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
    let client = build_http_client(&policy, input.allowed_domains.clone(), input.blocked_domains.clone())?;
    let response = client
        .get(request_url.clone())
        .send()
        .map_err(|error| error.to_string())?;

    let status = response.status();
    let final_url = response.url().to_string();
    let code = status.as_u16();
    let code_text = status.canonical_reason().unwrap_or("Unknown").to_string();
    if let Ok(final_receipt) = policy.enforce_url_with_filters(
        &final_url, input.allowed_domains.as_deref(), input.blocked_domains.as_deref(),
    ) {
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
    drop(client);
    let bytes = body.len();
    {
        use sha2::{Digest, Sha256};
        lease.finish_local_network_operation(operation, harness_contract::context::LocalNetworkTerminal::BodyDone {
            status: code,
            body_bytes: bytes as u64,
            body_sha256: format!("{:x}", Sha256::digest(body.as_bytes())),
        })?;
    }
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

#[cfg(test)]
mod effective_fetch_redirect_tests {
    use super::*;

    struct PrivateNetworkEnv(Option<std::ffi::OsString>);

    impl Drop for PrivateNetworkEnv {
        fn drop(&mut self) {
            match self.0.take() {
                Some(value) => std::env::set_var("COWD_ALLOW_PRIVATE_NETWORK", value),
                None => std::env::remove_var("COWD_ALLOW_PRIVATE_NETWORK"),
            }
        }
    }

    #[test]
    fn redirect_filter_refuses_target_before_connecting() {
        use std::io::{Read, Write};
        let _lock = crate::test_process_environment_lock().lock().unwrap();
        let _environment = PrivateNetworkEnv(std::env::var_os("COWD_ALLOW_PRIVATE_NETWORK"));
        std::env::set_var("COWD_ALLOW_PRIVATE_NETWORK", "1");
        let origin = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        origin.set_nonblocking(true).unwrap();
        let target = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        target.set_nonblocking(true).unwrap();
        let origin_url = format!("http://{}/redirect", origin.local_addr().unwrap());
        let target_url = format!("http://localhost:{}/blocked", target.local_addr().unwrap().port());
        let server = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(3);
            let mut stream = loop {
                match origin.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "origin was never contacted");
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("origin accept failed: {error}"),
                }
            };
            stream.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
            let mut bytes = [0; 4096];
            assert!(stream.read(&mut bytes).unwrap() > 0);
            write!(stream, "HTTP/1.1 302 Found\r\nLocation: {target_url}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
        });
        let client = build_http_client(
            &crate::network_policy::NetworkDomainPolicy::default(), None, Some(vec!["localhost".into()]),
        ).unwrap();
        let result = client.get(origin_url).send();
        server.join().unwrap();
        let error = result.expect_err("blocked redirect must not be followed");
        assert!(error.is_redirect(), "{error}");
        assert!(matches!(target.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock));
    }
}

fn build_http_client(
    policy: &crate::network_policy::NetworkDomainPolicy,
    allowed: Option<Vec<String>>,
    blocked: Option<Vec<String>>,
) -> Result<Client, String> {
    let policy = policy.clone();
    Client::builder()
        .timeout(Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::custom(move |attempt| {
            if attempt.previous().len() > 10 {
                return attempt.error("too many redirects");
            }
            match policy.enforce_url_with_filters(attempt.url().as_str(), allowed.as_deref(), blocked.as_deref()) {
                Ok(receipt) if !receipt.denied && !receipt.requires_approval => attempt.follow(),
                _ => attempt.error("redirect target refused by effective network policy"),
            }
        }))
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
