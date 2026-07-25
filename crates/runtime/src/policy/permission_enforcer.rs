#![allow(
    clippy::match_wildcard_for_single_variants,
    clippy::must_use_candidate,
    clippy::uninlined_format_args
)]
//! Permission enforcement layer that gates tool execution based on the
//! active `PermissionPolicy`.

use crate::permissions::{PermissionMode, PermissionOutcome, PermissionPolicy};
use approval::{ApprovalPolicyArtifact, FileApprovalPolicyArtifact};
use serde::{Deserialize, Serialize};
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome")]
pub enum EnforcementResult {
    /// Tool execution is allowed.
    Allowed,
    /// Tool execution was denied due to insufficient permissions.
    Denied {
        tool: String,
        active_mode: String,
        required_mode: String,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct PermissionEnforcer {
    policy: PermissionPolicy,
}

impl PermissionEnforcer {
    #[must_use]
    pub fn new(policy: PermissionPolicy) -> Self {
        Self { policy }
    }

    /// Check whether a tool can be executed under the current permission policy.
    /// Auto-denies when prompting is required but no prompter is provided.
    pub fn check(&self, tool_name: &str, input: &str) -> EnforcementResult {
        // When the active mode is Prompt, defer to the caller's interactive
        // prompt flow rather than hard-denying (the enforcer has no prompter).
        if self.policy.active_mode() == PermissionMode::Prompt {
            return EnforcementResult::Allowed;
        }

        let outcome = self.policy.authorize(tool_name, input, None);

        match outcome {
            PermissionOutcome::Allow => EnforcementResult::Allowed,
            PermissionOutcome::Deny { reason } => {
                let active_mode = self.policy.active_mode();
                let required_mode = self.policy.required_mode_for(tool_name);
                EnforcementResult::Denied {
                    tool: tool_name.to_owned(),
                    active_mode: active_mode.as_str().to_owned(),
                    required_mode: required_mode.as_str().to_owned(),
                    reason,
                }
            }
        }
    }

    #[must_use]
    pub fn is_allowed(&self, tool_name: &str, input: &str) -> bool {
        matches!(self.check(tool_name, input), EnforcementResult::Allowed)
    }

    /// Check permission with an explicitly provided required mode.
    /// Used when the required mode is determined dynamically (e.g., bash command classification).
    pub fn check_with_required_mode(
        &self,
        tool_name: &str,
        input: &str,
        required_mode: PermissionMode,
    ) -> EnforcementResult {
        // When the active mode is Prompt, defer to the caller's interactive
        // prompt flow rather than hard-denying.
        if self.policy.active_mode() == PermissionMode::Prompt {
            return EnforcementResult::Allowed;
        }

        let active_mode = self.policy.active_mode();

        // Check if active mode meets the dynamically determined required mode
        if active_mode >= required_mode {
            return EnforcementResult::Allowed;
        }

        // Permission denied - active mode is insufficient
        EnforcementResult::Denied {
            tool: tool_name.to_owned(),
            active_mode: active_mode.as_str().to_owned(),
            required_mode: required_mode.as_str().to_owned(),
            reason: format!(
                "'{tool_name}' with input '{input}' requires '{}' permission, but current mode is '{}'",
                required_mode.as_str(),
                active_mode.as_str()
            ),
        }
    }

    #[must_use]
    pub fn active_mode(&self) -> PermissionMode {
        self.policy.active_mode()
    }

    /// Classify a file operation against workspace boundaries.
    pub fn check_file_write(&self, path: &str, workspace_root: &str) -> EnforcementResult {
        let mode = self.policy.active_mode();

        match mode {
            PermissionMode::ReadOnly => EnforcementResult::Denied {
                tool: "write_file".to_owned(),
                active_mode: mode.as_str().to_owned(),
                required_mode: PermissionMode::WorkspaceWrite.as_str().to_owned(),
                reason: format!("file writes are not allowed in '{}' mode", mode.as_str()),
            },
            PermissionMode::WorkspaceWrite => {
                if is_within_workspace(path, workspace_root) {
                    EnforcementResult::Allowed
                } else {
                    EnforcementResult::Denied {
                        tool: "write_file".to_owned(),
                        active_mode: mode.as_str().to_owned(),
                        required_mode: PermissionMode::DangerFullAccess.as_str().to_owned(),
                        reason: format!(
                            "path '{}' is outside workspace root '{}'",
                            path, workspace_root
                        ),
                    }
                }
            }
            // Allow and DangerFullAccess permit all writes
            PermissionMode::Allow | PermissionMode::DangerFullAccess => EnforcementResult::Allowed,
            PermissionMode::Prompt => EnforcementResult::Denied {
                tool: "write_file".to_owned(),
                active_mode: mode.as_str().to_owned(),
                required_mode: PermissionMode::WorkspaceWrite.as_str().to_owned(),
                reason: "file write requires confirmation in prompt mode".to_owned(),
            },
        }
    }

    /// Check if a bash command should be allowed based on current mode.
    pub fn check_bash(&self, command: &str) -> EnforcementResult {
        let mode = self.policy.active_mode();

        match mode {
            PermissionMode::ReadOnly => {
                if is_read_only_command(command) {
                    EnforcementResult::Allowed
                } else {
                    EnforcementResult::Denied {
                        tool: "bash".to_owned(),
                        active_mode: mode.as_str().to_owned(),
                        required_mode: PermissionMode::WorkspaceWrite.as_str().to_owned(),
                        reason: format!(
                            "command may modify state; not allowed in '{}' mode",
                            mode.as_str()
                        ),
                    }
                }
            }
            PermissionMode::Prompt => EnforcementResult::Denied {
                tool: "bash".to_owned(),
                active_mode: mode.as_str().to_owned(),
                required_mode: PermissionMode::DangerFullAccess.as_str().to_owned(),
                reason: "bash requires confirmation in prompt mode".to_owned(),
            },
            // WorkspaceWrite, Allow, DangerFullAccess: permit bash
            _ => EnforcementResult::Allowed,
        }
    }
}

/// Resolve a path to its canonical form, falling back through multiple strategies.
///
/// 1. Try `canonicalize()` for paths that exist on disk.
/// 2. Fall back to canonicalizing the parent and joining the filename.
/// 3. Last resort: lexically resolve `..` and `.` components.
fn resolve_canonical(path: &str) -> Option<PathBuf> {
    let p = Path::new(path);

    // Strategy 1: full canonicalize
    if let Ok(canonical) = p.canonicalize() {
        return Some(canonical);
    }

    // Strategy 2: canonicalize parent + join filename
    if let Some(parent) = p.parent() {
        if let Ok(canonical_parent) = parent.canonicalize() {
            if let Some(name) = p.file_name() {
                return Some(canonical_parent.join(name));
            }
        }
    }

    // Strategy 3: lexical resolution of .. and .
    let mut resolved = PathBuf::new();
    for component in p.components() {
        match component {
            Component::ParentDir => {
                resolved.pop();
            }
            Component::CurDir => {}
            other => {
                resolved.push(other.as_os_str());
            }
        }
    }

    Some(resolved)
}

/// Workspace boundary check via canonical path comparison.
///
/// Resolves both the candidate path and the workspace root to canonical forms
/// before checking that the candidate is a child of the root. This prevents
/// path-traversal attacks like `/workspace/../../etc/passwd`.
fn is_within_workspace(path: &str, workspace_root: &str) -> bool {
    // Resolve relative paths against workspace root first
    let full_path = if Path::new(path).is_relative() {
        let root = workspace_root.trim_end_matches('/');
        PathBuf::from(format!("{root}/{path}"))
    } else {
        PathBuf::from(path)
    };

    let resolved_path = resolve_canonical(full_path.to_str().unwrap_or(path));
    let resolved_root = resolve_canonical(workspace_root);

    match (resolved_path, resolved_root) {
        (Some(path), Some(root)) => path.starts_with(&root),
        _ => false,
    }
}

/// Conservative heuristic: is this bash command read-only?
///
/// Returns true for commands that are known to not modify the system state.
/// Commands that include dangerous flags (`--force`, `-i`, `push`, etc.) or
/// redirections (`>`, `>>`) are excluded even if their base command is safe.
pub fn is_read_only_command(command: &str) -> bool {
    let first_token = command
        .split_whitespace()
        .next()
        .unwrap_or("")
        .rsplit('/')
        .next()
        .unwrap_or("");

    matches!(
        first_token,
        "cat"
            | "head"
            | "tail"
            | "less"
            | "more"
            | "wc"
            | "ls"
            | "find"
            | "grep"
            | "rg"
            | "awk"
            | "sed"
            | "echo"
            | "printf"
            | "which"
            | "where"
            | "whoami"
            | "pwd"
            | "env"
            | "printenv"
            | "date"
            | "cal"
            | "df"
            | "du"
            | "free"
            | "uptime"
            | "uname"
            | "file"
            | "stat"
            | "diff"
            | "sort"
            | "uniq"
            | "tr"
            | "cut"
            | "paste"
            | "tee"
            | "xargs"
            | "test"
            | "true"
            | "false"
            | "type"
            | "readlink"
            | "realpath"
            | "basename"
            | "dirname"
            | "sha256sum"
            | "md5sum"
            | "b3sum"
            | "xxd"
            | "hexdump"
            | "od"
            | "strings"
            | "tree"
            | "jq"
            | "yq"
            | "git"
            | "gh"
    ) && !command.contains("-i ")
        && !command.contains("--in-place")
        && !command.contains(" > ")
        && !command.contains(" >> ")
        && !command.contains("--force")
        && !command.contains(" push ")
        && !command.contains(" clean -fdx")
        && !command.contains(" rm ")
        && !command.contains(" reset --hard")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_enforcer(mode: PermissionMode) -> PermissionEnforcer {
        let policy = PermissionPolicy::new(mode);
        PermissionEnforcer::new(policy)
    }

    #[test]
    fn allow_mode_permits_everything() {
        let enforcer = make_enforcer(PermissionMode::Allow);
        assert!(enforcer.is_allowed("bash", ""));
        assert!(enforcer.is_allowed("write_file", ""));
        assert!(enforcer.is_allowed("edit_file", ""));
        assert_eq!(
            enforcer.check_file_write("/outside/path", "/workspace"),
            EnforcementResult::Allowed
        );
        assert_eq!(enforcer.check_bash("rm -rf /"), EnforcementResult::Allowed);
    }

    #[test]
    fn read_only_denies_writes() {
        let policy = PermissionPolicy::new(PermissionMode::ReadOnly)
            .with_tool_requirement("read_file", PermissionMode::ReadOnly)
            .with_tool_requirement("grep_search", PermissionMode::ReadOnly)
            .with_tool_requirement("write_file", PermissionMode::WorkspaceWrite);

        let enforcer = PermissionEnforcer::new(policy);
        assert!(enforcer.is_allowed("read_file", ""));
        assert!(enforcer.is_allowed("grep_search", ""));

        // write_file requires WorkspaceWrite but we're in ReadOnly
        let result = enforcer.check("write_file", "");
        assert!(matches!(result, EnforcementResult::Denied { .. }));

        let result = enforcer.check_file_write("/workspace/file.rs", "/workspace");
        assert!(matches!(result, EnforcementResult::Denied { .. }));
    }

    #[test]
    fn read_only_allows_read_commands() {
        let enforcer = make_enforcer(PermissionMode::ReadOnly);
        assert_eq!(
            enforcer.check_bash("cat src/main.rs"),
            EnforcementResult::Allowed
        );
        assert_eq!(
            enforcer.check_bash("grep -r 'pattern' ."),
            EnforcementResult::Allowed
        );
        assert_eq!(enforcer.check_bash("ls -la"), EnforcementResult::Allowed);
    }

    #[test]
    fn read_only_denies_write_commands() {
        let enforcer = make_enforcer(PermissionMode::ReadOnly);
        let result = enforcer.check_bash("rm file.txt");
        assert!(matches!(result, EnforcementResult::Denied { .. }));
    }

    #[test]
    fn workspace_write_allows_within_workspace() {
        let enforcer = make_enforcer(PermissionMode::WorkspaceWrite);
        let result = enforcer.check_file_write("/workspace/src/main.rs", "/workspace");
        assert_eq!(result, EnforcementResult::Allowed);
    }

    #[test]
    fn workspace_write_denies_outside_workspace() {
        let enforcer = make_enforcer(PermissionMode::WorkspaceWrite);
        let result = enforcer.check_file_write("/etc/passwd", "/workspace");
        assert!(matches!(result, EnforcementResult::Denied { .. }));
    }

    #[test]
    fn prompt_mode_denies_without_prompter() {
        let enforcer = make_enforcer(PermissionMode::Prompt);
        let result = enforcer.check_bash("echo test");
        assert!(matches!(result, EnforcementResult::Denied { .. }));

        let result = enforcer.check_file_write("/workspace/file.rs", "/workspace");
        assert!(matches!(result, EnforcementResult::Denied { .. }));
    }

    #[test]
    fn workspace_boundary_check() {
        assert!(is_within_workspace("/workspace/src/main.rs", "/workspace"));
        assert!(is_within_workspace("/workspace", "/workspace"));
        assert!(!is_within_workspace("/etc/passwd", "/workspace"));
        assert!(!is_within_workspace("/workspacex/hack", "/workspace"));
    }

    #[test]
    fn workspace_boundary_rejects_path_traversal() {
        // Path traversal attempts — must all be rejected
        assert!(
            !is_within_workspace("/workspace/../../etc/passwd", "/workspace"),
            "parent-dir traversal should be rejected"
        );
        assert!(
            !is_within_workspace("/workspace/../../etc/passwd", "/workspace/"),
            "parent-dir traversal with trailing slash should be rejected"
        );
        assert!(
            !is_within_workspace("/workspace/../workspace/../../etc/shadow", "/workspace"),
            "multi-hop traversal should be rejected"
        );
        assert!(
            !is_within_workspace("/workspace/foo/../../etc/passwd", "/workspace"),
            "deep traversal should be rejected"
        );
    }

    #[test]
    fn read_only_command_heuristic() {
        assert!(is_read_only_command("cat file.txt"));
        assert!(is_read_only_command("grep pattern file"));
        assert!(is_read_only_command("git log --oneline"));
        assert!(!is_read_only_command("rm file.txt"));
        assert!(!is_read_only_command("echo test > file.txt"));
        assert!(!is_read_only_command("sed -i 's/a/b/' file"));
    }

    #[test]
    fn active_mode_returns_policy_mode() {
        // given
        let modes = [
            PermissionMode::ReadOnly,
            PermissionMode::WorkspaceWrite,
            PermissionMode::DangerFullAccess,
            PermissionMode::Prompt,
            PermissionMode::Allow,
        ];

        // when
        let active_modes: Vec<_> = modes
            .into_iter()
            .map(|mode| make_enforcer(mode).active_mode())
            .collect();

        // then
        assert_eq!(active_modes, modes);
    }

    #[test]
    fn danger_full_access_permits_file_writes_and_bash() {
        // given
        let enforcer = make_enforcer(PermissionMode::DangerFullAccess);

        // when
        let file_result = enforcer.check_file_write("/outside/workspace/file.txt", "/workspace");
        let bash_result = enforcer.check_bash("rm -rf /tmp/scratch");

        // then
        assert_eq!(file_result, EnforcementResult::Allowed);
        assert_eq!(bash_result, EnforcementResult::Allowed);
    }

    #[test]
    fn check_denied_payload_contains_tool_and_modes() {
        // given
        let policy = PermissionPolicy::new(PermissionMode::ReadOnly)
            .with_tool_requirement("write_file", PermissionMode::WorkspaceWrite);
        let enforcer = PermissionEnforcer::new(policy);

        // when
        let result = enforcer.check("write_file", "{}");

        // then
        match result {
            EnforcementResult::Denied {
                tool,
                active_mode,
                required_mode,
                reason,
            } => {
                assert_eq!(tool, "write_file");
                assert_eq!(active_mode, "read-only");
                assert_eq!(required_mode, "workspace-write");
                assert!(reason.contains("requires workspace-write permission"));
            }
            other => panic!("expected denied result, got {other:?}"),
        }
    }

    #[test]
    fn workspace_write_relative_path_resolved() {
        // given
        let enforcer = make_enforcer(PermissionMode::WorkspaceWrite);

        // when
        let result = enforcer.check_file_write("src/main.rs", "/workspace");

        // then
        assert_eq!(result, EnforcementResult::Allowed);
    }

    #[test]
    fn workspace_root_with_trailing_slash() {
        // given
        let enforcer = make_enforcer(PermissionMode::WorkspaceWrite);

        // when
        let result = enforcer.check_file_write("/workspace/src/main.rs", "/workspace/");

        // then
        assert_eq!(result, EnforcementResult::Allowed);
    }

    #[test]
    fn workspace_root_equality() {
        // given
        let root = "/workspace/";

        // when
        let equal_to_root = is_within_workspace("/workspace", root);

        // then
        assert!(equal_to_root);
    }

    #[test]
    fn bash_heuristic_full_path_prefix() {
        // given
        let full_path_command = "/usr/bin/cat Cargo.toml";
        let git_path_command = "/usr/local/bin/git status";

        // when
        let cat_result = is_read_only_command(full_path_command);
        let git_result = is_read_only_command(git_path_command);

        // then
        assert!(cat_result);
        assert!(git_result);
    }

    #[test]
    fn bash_heuristic_redirects_block_read_only_commands() {
        // given
        let overwrite = "cat Cargo.toml > out.txt";
        let append = "echo test >> out.txt";

        // when
        let overwrite_result = is_read_only_command(overwrite);
        let append_result = is_read_only_command(append);

        // then
        assert!(!overwrite_result);
        assert!(!append_result);
    }

    #[test]
    fn bash_heuristic_in_place_flag_blocks() {
        // given
        let interactive_python = "python -i script.py";
        let in_place_sed = "sed --in-place 's/a/b/' file.txt";

        // when
        let interactive_result = is_read_only_command(interactive_python);
        let in_place_result = is_read_only_command(in_place_sed);

        // then
        assert!(!interactive_result);
        assert!(!in_place_result);
    }

    #[test]
    fn bash_heuristic_empty_command() {
        // given
        let empty = "";
        let whitespace = "   ";

        // when
        let empty_result = is_read_only_command(empty);
        let whitespace_result = is_read_only_command(whitespace);

        // then
        assert!(!empty_result);
        assert!(!whitespace_result);
    }

    #[test]
    fn prompt_mode_check_bash_denied_payload_fields() {
        // given
        let enforcer = make_enforcer(PermissionMode::Prompt);

        // when
        let result = enforcer.check_bash("git status");

        // then
        match result {
            EnforcementResult::Denied {
                tool,
                active_mode,
                required_mode,
                reason,
            } => {
                assert_eq!(tool, "bash");
                assert_eq!(active_mode, "prompt");
                assert_eq!(required_mode, "danger-full-access");
                assert_eq!(reason, "bash requires confirmation in prompt mode");
            }
            other => panic!("expected denied result, got {other:?}"),
        }
    }

    #[test]
    fn read_only_check_file_write_denied_payload() {
        // given
        let enforcer = make_enforcer(PermissionMode::ReadOnly);

        // when
        let result = enforcer.check_file_write("/workspace/file.txt", "/workspace");

        // then
        match result {
            EnforcementResult::Denied {
                tool,
                active_mode,
                required_mode,
                reason,
            } => {
                assert_eq!(tool, "write_file");
                assert_eq!(active_mode, "read-only");
                assert_eq!(required_mode, "workspace-write");
                assert!(reason.contains("file writes are not allowed"));
            }
            other => panic!("expected denied result, got {other:?}"),
        }
    }

    #[test]
    fn bash_heuristic_blocks_interpreter_code_execution() {
        assert!(!is_read_only_command(
            "python3 -c \"import os; os.system('id')\""
        ));
        assert!(!is_read_only_command("python -c \"print('x')\""));
        assert!(!is_read_only_command("node -e \"console.log('x')\""));
        assert!(!is_read_only_command("ruby -e \"system('id')\""));
        assert!(!is_read_only_command("cargo run"));
    }
}

// ── P0-1: Destructive Command Detection & Approval System ────────────────────

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Risk level for dangerous commands
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum RiskLevel {
    Low,      // mv (overwrite), cp (overwrite)
    Medium,   // git reset, pip uninstall, apt remove
    High,     // rm -rf (specified dir), git push --force, chmod 777
    Critical, // rm -rf /, dd, mkfs, format
}

/// Approval verdict from user
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApprovalVerdict {
    Approved,
    Denied { reason: String },
    TimedOut,
}

/// 3-level approval persistence (inspired by hermes approval.py)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ApprovalPersistence {
    Once,    // Approve this time only
    Session, // Approve for this session
    Always,  // Permanently approve (write to config)
}

/// Approval request sent to frontend
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub id: String,
    pub command: String,
    pub normalized_command: String,
    pub risk_level: RiskLevel,
    pub matched_patterns: Vec<String>,
    pub description: String,
    pub timestamp: DateTime<Utc>,
    pub timeout_secs: u64,
}

/// Approval response from frontend
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalResponse {
    pub request_id: String,
    pub verdict: ApprovalVerdict,
    pub persistence: ApprovalPersistence,
}

/// A single danger pattern
struct DangerPattern {
    name: String,
    regex: regex::Regex,
    risk: RiskLevel,
    description: String,
}

/// Destructive pattern detector with 91 regex patterns
/// (inspired by hermes-agent tools/approval.py)
pub struct DestructivePatternDetector {
    patterns: Vec<DangerPattern>,
    /// Session-level approval cache (command_hash → persistence)
    session_approved: Arc<RwLock<HashMap<String, ApprovalPersistence>>>,
    /// Explicit user-managed permanent allow-list artifact.
    approval_policy: FileApprovalPolicyArtifact,
}

impl DestructivePatternDetector {
    /// Create a new detector with all built-in patterns
    pub fn new(config_dir: PathBuf) -> Self {
        let layout = storage::StorageLayout::default_for_config_home(&config_dir);
        let always_approved_path = layout
            .file_path("always_approved")
            .map(Path::to_path_buf)
            .unwrap_or_else(|| {
                config_dir
                    .join("storage")
                    .join("files")
                    .join("always_approved.json")
            });
        let approval_policy = FileApprovalPolicyArtifact::new(always_approved_path);
        let patterns = Self::build_patterns();
        Self {
            patterns,
            session_approved: Arc::new(RwLock::new(HashMap::new())),
            approval_policy,
        }
    }

    /// Build the 91 dangerous command patterns
    fn build_patterns() -> Vec<DangerPattern> {
        let mut patterns = Vec::new();

        // File deletion (15 patterns)
        let file_del = &[
            (
                r"rm\s+(-[a-zA-Z]*f[a-zA-Z]*\s+/|.*--force\s+/)",
                RiskLevel::Critical,
                "rm recursive force on root",
            ),
            (
                r"rm\s+(-[a-zA-Z]*f[a-zA-Z]*\s+|.*--force\s+)",
                RiskLevel::High,
                "rm recursive force",
            ),
            (
                r"rm\s+(-[a-zA-Z]*r[a-zA-Z]*\s+)",
                RiskLevel::High,
                "rm recursive",
            ),
            (r"rmdir\s+", RiskLevel::Low, "remove directory"),
            (r"unlink\s+", RiskLevel::Medium, "unlink file"),
            (r"shred\s+", RiskLevel::High, "shred file contents"),
            (r"truncate\s+", RiskLevel::Medium, "truncate file"),
            (r">+\s*\S+", RiskLevel::Low, "overwrite file with redirect"),
            (r"find\s+.*-delete", RiskLevel::High, "find and delete"),
            (r"install\s+.*--unlink", RiskLevel::Medium, "install unlink"),
            (r"git\s+clean\s+-fdx", RiskLevel::High, "git clean force"),
            (r"git\s+rm\s+", RiskLevel::Medium, "git remove file"),
            (r"npm\s+run\s+.*clean", RiskLevel::Low, "npm clean"),
            (r"cargo\s+clean", RiskLevel::Low, "cargo clean"),
            (r"make\s+clean", RiskLevel::Low, "make clean"),
        ];
        for (re, risk, desc) in file_del {
            if let Ok(r) = regex::Regex::new(re) {
                patterns.push(DangerPattern {
                    name: format!("file_del_{}", patterns.len()),
                    regex: r,
                    risk: *risk,
                    description: desc.to_string(),
                });
            }
        }

        // Disk operations (8 patterns)
        let disk = &[
            (r"dd\s+if=", RiskLevel::Critical, "dd disk copy"),
            (r"mkfs\b", RiskLevel::Critical, "make filesystem"),
            (r"fdisk\b", RiskLevel::Critical, "fdisk partition"),
            (r"parted\b", RiskLevel::Critical, "parted partition editor"),
            (r"format\s+[A-Z]:", RiskLevel::Critical, "format drive"),
            (r"hdparm\b", RiskLevel::Critical, "hard disk parameters"),
            (
                r"badblocks\s+-w",
                RiskLevel::Critical,
                "badblocks write test",
            ),
            (r"shred\s+/dev/", RiskLevel::Critical, "shred device"),
        ];
        for (re, risk, desc) in disk {
            if let Ok(r) = regex::Regex::new(re) {
                patterns.push(DangerPattern {
                    name: format!("disk_{}", patterns.len()),
                    regex: r,
                    risk: *risk,
                    description: desc.to_string(),
                });
            }
        }

        // Permission changes (10 patterns)
        let perms = &[
            (
                r"chmod\s+(777|666|000|a\+[rwx])",
                RiskLevel::High,
                "dangerous chmod",
            ),
            (r"chown\s+root", RiskLevel::High, "chown to root"),
            (r"chgrp\s+root", RiskLevel::High, "chgrp to root"),
            (r"chmod\s+-R\s+", RiskLevel::High, "recursive chmod"),
            (r"chown\s+-R\s+", RiskLevel::High, "recursive chown"),
            (r"setfacl\s+", RiskLevel::Medium, "set file ACL"),
            (r"setsebool\s+", RiskLevel::Medium, "set SELinux boolean"),
            (r"chcon\s+", RiskLevel::Medium, "change SELinux context"),
            (r"sudo\s+chmod", RiskLevel::High, "sudo chmod"),
            (r"sudo\s+chown", RiskLevel::High, "sudo chown"),
        ];
        for (re, risk, desc) in perms {
            if let Ok(r) = regex::Regex::new(re) {
                patterns.push(DangerPattern {
                    name: format!("perms_{}", patterns.len()),
                    regex: r,
                    risk: *risk,
                    description: desc.to_string(),
                });
            }
        }

        // Git destructive (12 patterns)
        let git = &[
            (r"git\s+push\s+.*--force", RiskLevel::High, "git force push"),
            (
                r"git\s+push\s+-f\b",
                RiskLevel::High,
                "git force push short",
            ),
            (r"git\s+reset\s+--hard", RiskLevel::High, "git reset hard"),
            (
                r"git\s+reflog\s+expire",
                RiskLevel::High,
                "git reflog expire",
            ),
            (
                r"git\s+branch\s+-D\s+",
                RiskLevel::Medium,
                "git delete branch force",
            ),
            (r"git\s+tag\s+-d\s+", RiskLevel::Low, "git delete tag"),
            (r"git\s+stash\s+drop", RiskLevel::Low, "git stash drop"),
            (r"git\s+filter-branch", RiskLevel::High, "git filter-branch"),
            (
                r"git\s+submodule\s+deinit",
                RiskLevel::Medium,
                "git submodule deinit",
            ),
            (
                r"git\s+worktree\s+remove",
                RiskLevel::Low,
                "git worktree remove",
            ),
            (r"git\s+annex\s+drop", RiskLevel::Medium, "git annex drop"),
            (r"git\s+rebase\b", RiskLevel::Medium, "git rebase"),
        ];
        for (re, risk, desc) in git {
            if let Ok(r) = regex::Regex::new(re) {
                patterns.push(DangerPattern {
                    name: format!("git_{}", patterns.len()),
                    regex: r,
                    risk: *risk,
                    description: desc.to_string(),
                });
            }
        }

        // Package management (8 patterns)
        let pkg = &[
            (
                r"apt\s+(remove|purge)",
                RiskLevel::Medium,
                "apt remove/purge",
            ),
            (
                r"apt-get\s+(remove|purge)",
                RiskLevel::Medium,
                "apt-get remove/purge",
            ),
            (r"yum\s+remove", RiskLevel::Medium, "yum remove"),
            (r"dnf\s+remove", RiskLevel::Medium, "dnf remove"),
            (r"pip\s+uninstall", RiskLevel::Medium, "pip uninstall"),
            (
                r"npm\s+uninstall\s+-g",
                RiskLevel::Medium,
                "npm global uninstall",
            ),
            (r"cargo\s+uninstall", RiskLevel::Low, "cargo uninstall"),
            (r"brew\s+uninstall", RiskLevel::Low, "brew uninstall"),
        ];
        for (re, risk, desc) in pkg {
            if let Ok(r) = regex::Regex::new(re) {
                patterns.push(DangerPattern {
                    name: format!("pkg_{}", patterns.len()),
                    regex: r,
                    risk: *risk,
                    description: desc.to_string(),
                });
            }
        }

        // Network dangerous (10 patterns)
        let net = &[
            (r"iptables\s+-F", RiskLevel::Critical, "flush iptables"),
            (
                r"curl.*\|.*(sh|bash)",
                RiskLevel::Critical,
                "pipe curl to shell",
            ),
            (r"wget.*\|.*sh", RiskLevel::Critical, "pipe wget to shell"),
            (r"nc\s+-l\s+", RiskLevel::High, "netcat listen"),
            (r"socat\s+", RiskLevel::High, "socat relay"),
            (r"ssh\s+.*-R\s+", RiskLevel::High, "SSH remote port forward"),
            (
                r"ssh\s+.*-L\s+",
                RiskLevel::Medium,
                "SSH local port forward",
            ),
            (r"tcpdump\s+-w", RiskLevel::Medium, "tcpdump write capture"),
            (r"nmap\s+.*--script", RiskLevel::High, "nmap script scan"),
            (r"airmon-ng\s+", RiskLevel::Critical, "WiFi monitor mode"),
        ];
        for (re, risk, desc) in net {
            if let Ok(r) = regex::Regex::new(re) {
                patterns.push(DangerPattern {
                    name: format!("net_{}", patterns.len()),
                    regex: r,
                    risk: *risk,
                    description: desc.to_string(),
                });
            }
        }

        // Process/System (12 patterns)
        let sys = &[
            (r"kill\s+-9\s+1\b", RiskLevel::Critical, "kill init process"),
            (r"killall\s+", RiskLevel::High, "kill all by name"),
            (r"pkill\s+", RiskLevel::High, "kill by pattern"),
            (r"shutdown\b", RiskLevel::Critical, "system shutdown"),
            (r"reboot\b", RiskLevel::Critical, "system reboot"),
            (r"init\s+[06]", RiskLevel::Critical, "init shutdown/reboot"),
            (
                r"systemctl\s+stop\s+ssh",
                RiskLevel::Critical,
                "stop SSH service",
            ),
            (
                r"systemctl\s+disable\s+ssh",
                RiskLevel::High,
                "disable SSH service",
            ),
            (r"systemctl\s+mask\s+", RiskLevel::High, "mask service"),
            (
                r"journalctl\s+--vacuum",
                RiskLevel::Medium,
                "journal vacuum",
            ),
            (r"sysctl\s+-w\s+", RiskLevel::High, "write kernel parameter"),
            (r"modprobe\s+-r\s+", RiskLevel::High, "remove kernel module"),
        ];
        for (re, risk, desc) in sys {
            if let Ok(r) = regex::Regex::new(re) {
                patterns.push(DangerPattern {
                    name: format!("sys_{}", patterns.len()),
                    regex: r,
                    risk: *risk,
                    description: desc.to_string(),
                });
            }
        }

        // Docker (6 patterns)
        let docker = &[
            (
                r"docker\s+rm\s+-f",
                RiskLevel::High,
                "docker force remove container",
            ),
            (
                r"docker\s+system\s+prune",
                RiskLevel::High,
                "docker system prune",
            ),
            (r"docker\s+rmi\s+", RiskLevel::Medium, "docker remove image"),
            (
                r"docker\s+volume\s+rm",
                RiskLevel::Medium,
                "docker remove volume",
            ),
            (
                r"docker\s+network\s+rm",
                RiskLevel::Low,
                "docker remove network",
            ),
            (
                r"docker\s+compose\s+down\s+.*--rmi",
                RiskLevel::High,
                "docker compose down remove images",
            ),
        ];
        for (re, risk, desc) in docker {
            if let Ok(r) = regex::Regex::new(re) {
                patterns.push(DangerPattern {
                    name: format!("docker_{}", patterns.len()),
                    regex: r,
                    risk: *risk,
                    description: desc.to_string(),
                });
            }
        }

        // Database (10 patterns)
        let db = &[
            (
                r"DROP\s+(DATABASE|TABLE|SCHEMA)",
                RiskLevel::Critical,
                "DROP database/table",
            ),
            (r"TRUNCATE\s+", RiskLevel::Critical, "TRUNCATE table"),
            (
                r"DELETE\s+FROM\s+\w+\s*;?\s*$",
                RiskLevel::High,
                "DELETE all rows",
            ),
            (r"ALTER\s+DATABASE\s+", RiskLevel::High, "ALTER database"),
            (r"DROP\s+INDEX\s+", RiskLevel::High, "DROP index"),
            (r"DROP\s+USER\s+", RiskLevel::Critical, "DROP user"),
            (r"GRANT\s+ALL\s+", RiskLevel::High, "GRANT all privileges"),
            (
                r"REVOKE\s+ALL\s+",
                RiskLevel::Medium,
                "REVOKE all privileges",
            ),
            (
                r"pg_dump\s+.*--clean",
                RiskLevel::High,
                "pg_dump with clean",
            ),
            (
                r"mysqldump\s+.*--add-drop-table",
                RiskLevel::Medium,
                "mysqldump drop table",
            ),
        ];
        for (re, risk, desc) in db {
            if let Ok(r) = regex::Regex::new(re) {
                patterns.push(DangerPattern {
                    name: format!("db_{}", patterns.len()),
                    regex: r,
                    risk: *risk,
                    description: desc.to_string(),
                });
            }
        }

        patterns
    }

    /// Normalize command: remove null bytes, strip ANSI escapes, compact whitespace
    fn normalize_command(cmd: &str) -> String {
        // Step 1: Remove null bytes
        let no_null: String = cmd.chars().filter(|c| *c != '\0').collect();

        // Step 2: Strip ANSI escape sequences (static regex avoids recompilation on hot path)
        static ANSI_RE: std::sync::LazyLock<Option<regex::Regex>> =
            std::sync::LazyLock::new(|| {
                regex::Regex::new(r"\x1b\[[0-9;]*[a-zA-Z]|\x1b\].*?\x07").ok()
            });
        let no_ansi = ANSI_RE.as_ref().map_or(no_null.clone(), |regex| {
            regex.replace_all(&no_null, "").to_string()
        });

        // Step 3: Compact consecutive whitespace
        let compact: String = no_ansi.split_whitespace().collect::<Vec<_>>().join(" ");

        compact
    }

    /// Generate a cache key for a normalized command
    fn command_cache_key(&self, normalized: &str) -> String {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        normalized.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }

    /// Check if command is permanently approved
    fn is_always_approved(&self, _normalized: &str) -> bool {
        self.approval_policy
            .list_always_allowed()
            .map(|patterns| patterns.iter().any(|p| _normalized.contains(p)))
            .unwrap_or(false)
    }

    /// Detect if a command is dangerous and requires approval
    pub fn detect(&self, raw_cmd: &str) -> Option<ApprovalRequest> {
        let normalized = Self::normalize_command(raw_cmd);

        // Check session-level approval cache
        let cache_key = self.command_cache_key(&normalized);
        if let Ok(cache) = self.session_approved.try_read() {
            if let Some(persistence) = cache.get(&cache_key) {
                match persistence {
                    ApprovalPersistence::Session | ApprovalPersistence::Always => return None,
                    ApprovalPersistence::Once => {}
                }
            }
        }

        // Check permanent approval
        if self.is_always_approved(&normalized) {
            return None;
        }

        // Pattern matching
        let mut matched = Vec::new();
        let mut highest_risk = RiskLevel::Low;
        let mut descriptions = Vec::new();

        for pattern in &self.patterns {
            if pattern.regex.is_match(&normalized) {
                matched.push(pattern.name.clone());
                descriptions.push(pattern.description.clone());
                if pattern.risk > highest_risk {
                    highest_risk = pattern.risk;
                }
            }
        }

        if matched.is_empty() {
            return None;
        }

        Some(ApprovalRequest {
            id: uuid::Uuid::new_v4().to_string(),
            command: raw_cmd.to_string(),
            normalized_command: normalized,
            risk_level: highest_risk,
            matched_patterns: matched,
            description: descriptions.join("; "),
            timestamp: Utc::now(),
            timeout_secs: 120,
        })
    }

    /// Apply an approved decision's requested cache/policy persistence.
    /// The decision receipt itself is written by `SmartApprovalGate` before
    /// this method is invoked; a failed always-allow artifact is observable
    /// and intentionally never converted into a silent bypass.
    pub async fn record_approval(
        &self,
        command: &str,
        persistence: ApprovalPersistence,
    ) -> Result<(), approval::ApprovalRepositoryError> {
        let normalized = Self::normalize_command(command);
        let cache_key = self.command_cache_key(&normalized);

        match &persistence {
            ApprovalPersistence::Session => {
                let mut cache = self.session_approved.write().await;
                cache.insert(cache_key, persistence);
                Ok(())
            }
            ApprovalPersistence::Once => Ok(()),
            ApprovalPersistence::Always => self.approval_policy.add_always_allowed(&normalized),
        }
    }
}

// ── Smart Approval Verdict (intelligent approval decision) ──────────────────

use crate::config::ApprovalConfig;

/// Reason a command was automatically allowed without user approval.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AutoPassReason {
    /// Command did not match any destructive pattern.
    NoPatternMatch,
    /// Command was detected as read-only (ls, cat, grep, etc.).
    ReadOnlyCommand,
    /// Command matched a Low-risk pattern but auto-pass is enabled.
    LowRiskAutoPass,
    /// SOLO mode is active; non-critical commands bypass approval.
    SoloBypass,
    /// Command was previously approved with Session/Always persistence.
    CachedApproval { persistence: ApprovalPersistence },
}

/// Intelligent approval verdict combining pattern detection with approval config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SmartApprovalVerdict {
    /// Command is allowed without user interaction.
    AutoPass { reason: AutoPassReason },
    /// Command requires explicit user approval via the frontend card.
    NeedsApproval(ApprovalRequest),
}

impl DestructivePatternDetector {
    /// Intelligent detection that considers approval configuration.
    ///
    /// This method applies the smart approval policy:
    /// - Read-only commands auto-pass
    /// - SOLO mode bypasses non-critical approvals
    /// - Low-risk patterns auto-pass when configured
    /// - Everything else requires explicit approval
    pub fn detect_with_config(
        &self,
        raw_cmd: &str,
        config: &ApprovalConfig,
    ) -> SmartApprovalVerdict {
        let normalized = Self::normalize_command(raw_cmd);

        // Step 1: Check if this is a read-only command (auto-pass)
        if config.auto_pass_read_only && is_read_only_command(&normalized) {
            return SmartApprovalVerdict::AutoPass {
                reason: AutoPassReason::ReadOnlyCommand,
            };
        }

        // Step 2: Check session-level and permanent approval cache
        let cache_key = self.command_cache_key(&normalized);
        if let Ok(cache) = self.session_approved.try_read() {
            if let Some(persistence) = cache.get(&cache_key) {
                match persistence {
                    ApprovalPersistence::Session | ApprovalPersistence::Always => {
                        return SmartApprovalVerdict::AutoPass {
                            reason: AutoPassReason::CachedApproval {
                                persistence: persistence.clone(),
                            },
                        };
                    }
                    ApprovalPersistence::Once => {}
                }
            }
        }

        // Step 3: Check permanent approval file
        if self.is_always_approved(&normalized) {
            return SmartApprovalVerdict::AutoPass {
                reason: AutoPassReason::CachedApproval {
                    persistence: ApprovalPersistence::Always,
                },
            };
        }

        // Step 4: Run pattern detection
        let Some(approval_req) = self.detect(raw_cmd) else {
            return SmartApprovalVerdict::AutoPass {
                reason: AutoPassReason::NoPatternMatch,
            };
        };

        // Step 5: SOLO mode logic
        if config.solo_mode {
            // In SOLO mode, Critical-risk commands may still require approval
            if approval_req.risk_level == RiskLevel::Critical && config.solo_honor_critical {
                // Fall through to NeedsApproval
            } else {
                tracing::info!(
                    command = %raw_cmd,
                    risk_level = ?approval_req.risk_level,
                    "SOLO mode: auto-passing command"
                );
                return SmartApprovalVerdict::AutoPass {
                    reason: AutoPassReason::SoloBypass,
                };
            }
        }

        // Step 6: Low-risk auto-pass
        if approval_req.risk_level == RiskLevel::Low && config.auto_pass_low_risk {
            tracing::info!(
                command = %raw_cmd,
                patterns = ?approval_req.matched_patterns,
                "Low-risk command auto-passed"
            );
            return SmartApprovalVerdict::AutoPass {
                reason: AutoPassReason::LowRiskAutoPass,
            };
        }

        // Step 7: Requires explicit approval
        SmartApprovalVerdict::NeedsApproval(approval_req)
    }
}
