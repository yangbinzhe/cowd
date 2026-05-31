//! Brand constants and path helpers for the Cowd project.
//!
//! All directory names, file names, environment variable prefixes, and binary
//! names are defined here. Every crate should reference these constants instead
//! of hard-coding brand strings, so that a single change propagates everywhere.
//!
//! # Environment variable override
//!
//! The project dot-directory name defaults to `.cowd` but can be overridden
//! via the `COWD_DIR_NAME` environment variable (e.g. `COWD_DIR_NAME=.myorg`).
//! This allows organisations or individuals to customise the workspace folder
//! name without recompiling.
//!
//! # Directory layering
//!
//! Cowd uses a three-layer directory model, inspired by Claude Code and Opencode:
//!
//! L1 — User-level (`~/.cowd/`): global config, sessions, sandbox, agents, skills, plugins, credentials.
//! L2 — Project-level (`<project>/.cowd/`): project-specific config, CLAUDE.md, AGENTS.md.
//! L3 — Local override (`<project>/.cowd/config.local.*`): machine-local overrides.

use std::path::{Path, PathBuf};

/// Default dot-directory name used for project-level and user-level config.
///
/// Override with `COWD_DIR_NAME` env var. Always includes the leading dot.
pub fn dot_dir() -> String {
    std::env::var("COWD_DIR_NAME")
        .unwrap_or_else(|_| ".cowd".to_string())
}

/// Project-level dot-directory path under `cwd`.
pub fn project_dot_dir(cwd: &Path) -> PathBuf {
    cwd.join(dot_dir())
}

/// User-level dot-directory path under home.
pub fn user_dot_dir(home: &Path) -> PathBuf {
    home.join(dot_dir())
}

/// Resolve the user's home directory.
pub fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

/// Resolve the default `~/.cowd` config home directory.
/// Respects `COWD_CONFIG_HOME` env var override.
pub fn config_home_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("COWD_CONFIG_HOME") {
        return PathBuf::from(path);
    }
    user_dot_dir(&home_dir())
}

// ── Subdirectory names (constants) ──

pub const ENV_PREFIX: &str = "COWD_";
pub const BIN_NAME: &str = "cowd";
pub const CONFIG_FILE_YAML: &str = "config.yaml";
pub const SETTINGS_SCHEMA_NAME: &str = "CowdSettingsSchema";

/// Subdirectory names within the `.cowd` directory.
pub const AGENTS_DIR: &str = "agents";
pub const SKILLS_DIR: &str = "skills";
pub const PLUGINS_DIR: &str = "plugins";
pub const CREDENTIALS_DIR: &str = "credentials";
pub const WORKER_STATE_FILE: &str = "worker-state.json";
pub const SESSION_EXT: &str = ".jsonl";

// ── Session paths ──

const SESSIONS_DIR: &str = "sessions";
const GLOBAL_SESSIONS_DIR: &str = "global";
const PROJECT_SESSIONS_DIR: &str = "projects";

/// User-level global sessions: `~/.cowd/sessions/global/`
pub fn user_sessions_dir() -> PathBuf {
    config_home_dir().join(SESSIONS_DIR).join(GLOBAL_SESSIONS_DIR)
}

/// User-level project-scoped sessions: `~/.cowd/sessions/projects/<hash>/`
pub fn user_project_sessions_dir(fingerprint: &str) -> PathBuf {
    config_home_dir()
        .join(SESSIONS_DIR)
        .join(PROJECT_SESSIONS_DIR)
        .join(fingerprint)
}

/// Project-level sessions: `<project>/.cowd/sessions/<hash>/` (opt-in)
pub fn project_sessions_dir(cwd: &Path, fingerprint: &str) -> PathBuf {
    project_dot_dir(cwd).join(SESSIONS_DIR).join(fingerprint)
}

// ── Sandbox paths ──

const SANDBOX_DIR: &str = "sandbox";
const SANDBOX_HOME_SUBDIR: &str = "home";
const SANDBOX_TMP_SUBDIR: &str = "tmp";

/// User-level sandbox home: `~/.cowd/sandbox/home/`
pub fn sandbox_home_dir() -> PathBuf {
    config_home_dir().join(SANDBOX_DIR).join(SANDBOX_HOME_SUBDIR)
}

/// User-level sandbox tmp: `~/.cowd/sandbox/tmp/`
pub fn sandbox_tmp_dir() -> PathBuf {
    config_home_dir().join(SANDBOX_DIR).join(SANDBOX_TMP_SUBDIR)
}

// ── Cron paths ──

const CRON_DIR: &str = "cron";
const CRON_JOBS_FILE: &str = "jobs.json";

/// User-level cron jobs file: `~/.cowd/cron/jobs.json`
pub fn cron_jobs_path() -> PathBuf {
    config_home_dir().join(CRON_DIR).join(CRON_JOBS_FILE)
}

// ── Handoff paths ──

const HANDOFFS_DIR: &str = "handoffs";

/// User-level handoff directory: `~/.cowd/handoffs/`
pub fn handoffs_dir() -> PathBuf {
    config_home_dir().join(HANDOFFS_DIR)
}

// ── Worker-state paths ──

/// User-level worker state file: `~/.cowd/worker-state.json`
pub fn worker_state_path() -> PathBuf {
    config_home_dir().join(WORKER_STATE_FILE)
}

// ── User-level install dirs ──

/// User-level agents: `~/.cowd/agents/`
pub fn user_agents_dir() -> PathBuf {
    config_home_dir().join(AGENTS_DIR)
}

/// User-level skills: `~/.cowd/skills/`
pub fn user_skills_dir() -> PathBuf {
    config_home_dir().join(SKILLS_DIR)
}

/// User-level plugins: `~/.cowd/plugins/`
pub fn user_plugins_dir() -> PathBuf {
    config_home_dir().join(PLUGINS_DIR)
}

/// User-level credentials: `~/.cowd/credentials/`
pub fn user_credentials_dir() -> PathBuf {
    config_home_dir().join(CREDENTIALS_DIR)
}

/// Ensure all user-level directories exist.
pub fn ensure_user_dirs() -> std::io::Result<()> {
    use std::fs;
    let dirs = [
        user_sessions_dir(),
        sandbox_home_dir(),
        sandbox_tmp_dir(),
        user_agents_dir(),
        user_skills_dir(),
        user_plugins_dir(),
        user_credentials_dir(),
        cron_jobs_path().parent().unwrap().to_path_buf(),
        handoffs_dir(),
    ];
    for d in &dirs {
        fs::create_dir_all(d)?;
    }
    Ok(())
}

// ── Legacy sandbox dir constants (deprecated, project-local) ──
// Kept for migration detection; new code should use sandbox_home_dir() / sandbox_tmp_dir()
pub const SANDBOX_HOME_DIR_LEGACY: &str = "sandbox-home";
pub const SANDBOX_TMP_DIR_LEGACY: &str = "sandbox-tmp";

/// Check if the project `.cowd` directory has legacy sandbox or session dirs
/// that should be migrated to user-level.
pub fn has_legacy_user_data(cwd: &Path) -> bool {
    let p = project_dot_dir(cwd);
    p.join(SANDBOX_HOME_DIR_LEGACY).exists()
        || p.join(SANDBOX_TMP_DIR_LEGACY).exists()
        || p.join(SESSIONS_DIR).exists()
}

/// Helper: build the env var name for a given key using the COWD_ prefix.
pub fn env_var(key: &str) -> String {
    let mut buf = String::with_capacity(ENV_PREFIX.len() + key.len());
    buf.push_str(ENV_PREFIX);
    buf.push_str(key);
    buf
}

/// Expand a leading `~` in a path to the user's home directory.
/// Returns the original path unchanged if no `~` prefix is found.
pub fn expand_tilde(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        return home_dir().join(rest);
    }
    if path == "~" {
        return home_dir();
    }
    PathBuf::from(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn default_dot_dir_is_cowd() {
        if std::env::var("COWD_DIR_NAME").is_err() {
            assert_eq!(dot_dir(), ".cowd");
        }
    }

    #[test]
    fn env_var_builds_prefix() {
        assert_eq!(env_var("CONFIG_HOME"), "COWD_CONFIG_HOME");
    }

    #[test]
    fn project_dot_dir_joins() {
        let cwd = Path::new("/tmp/workspace");
        if std::env::var("COWD_DIR_NAME").is_err() {
            assert_eq!(project_dot_dir(cwd), PathBuf::from("/tmp/workspace/.cowd"));
        }
    }

    #[test]
    fn sandbox_dirs_resolve_under_config_home() {
        let home = sandbox_home_dir();
        let tmp = sandbox_tmp_dir();
        assert!(home.to_string_lossy().contains("sandbox/home"));
        assert!(tmp.to_string_lossy().contains("sandbox/tmp"));
    }

    #[test]
    fn user_sessions_dir_resolves_under_config_home() {
        let path = user_sessions_dir();
        assert!(path.to_string_lossy().contains("sessions/global"));
    }

    #[test]
    fn user_project_sessions_contains_fingerprint() {
        let path = user_project_sessions_dir("a1b2c3");
        assert!(path.to_string_lossy().contains("projects/a1b2c3"));
    }

    #[test]
    fn home_dir_returns_something() {
        let h = home_dir();
        assert!(!h.as_os_str().is_empty());
    }

    #[test]
    fn expand_tilde_expands_home() {
        let home = std::env::var("HOME").unwrap();
        let result = expand_tilde("~");
        assert_eq!(result, PathBuf::from(&home));
    }

    #[test]
    fn expand_tilde_expands_home_slash_path() {
        let home = std::env::var("HOME").unwrap();
        let result = expand_tilde("~/some/path");
        assert_eq!(result, PathBuf::from(&home).join("some/path"));
    }

    #[test]
    fn expand_tilde_passes_absolute_path_unchanged() {
        let result = expand_tilde("/absolute/path");
        assert_eq!(result, PathBuf::from("/absolute/path"));
    }

    #[test]
    fn expand_tilde_passes_empty_string() {
        let result = expand_tilde("");
        assert_eq!(result, PathBuf::from(""));
    }
}
