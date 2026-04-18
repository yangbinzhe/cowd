//! Brand constants for the Cowd project.
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

/// Default dot-directory name used for project-level and user-level config.
///
/// Override with `COWD_DIR_NAME` env var. Always includes the leading dot.
pub fn dot_dir() -> String {
    std::env::var("COWD_DIR_NAME")
        .unwrap_or_else(|_| ".cowd".to_string())
}

/// Project-level dot-directory path under `cwd`.
pub fn project_dot_dir(cwd: &std::path::Path) -> std::path::PathBuf {
    cwd.join(dot_dir())
}

/// User-level dot-directory path under home.
pub fn user_dot_dir(home: &std::path::Path) -> std::path::PathBuf {
    home.join(dot_dir())
}

/// Environment variable prefix for all Cowd-specific env vars.
pub const ENV_PREFIX: &str = "COWD_";

/// Binary name.
pub const BIN_NAME: &str = "cowd";

/// Main config file name (inside the dot-directory).
pub const CONFIG_FILE_JSON: &str = "cowd.json";

/// YAML config file name (inside the dot-directory).
pub const CONFIG_FILE_YAML: &str = "config.yaml";

/// Settings file name (legacy compatibility, inside the dot-directory).
pub const SETTINGS_FILE: &str = "settings.json";

/// Schema name advertised by generated settings files.
pub const SETTINGS_SCHEMA_NAME: &str = "CowdSettingsSchema";

/// Subdirectory name for agents within the dot-directory.
pub const AGENTS_DIR: &str = "agents";

/// Subdirectory name for skills within the dot-directory.
pub const SKILLS_DIR: &str = "skills";

/// Subdirectory name for sandbox HOME within the dot-directory.
pub const SANDBOX_HOME_DIR: &str = "sandbox-home";

/// Subdirectory name for sandbox TMP within the dot-directory.
pub const SANDBOX_TMP_DIR: &str = "sandbox-tmp";

/// Subdirectory name for worker state within the dot-directory.
pub const WORKER_STATE_FILE: &str = "worker-state.json";

/// Session file extension.
pub const SESSION_EXT: &str = ".jsonl";

/// Helper: build the env var name for a given key using the COWD_ prefix.
///
/// # Example
/// ```
/// assert_eq!(cowd_dirs::env_var("CONFIG_HOME"), "COWD_CONFIG_HOME");
/// ```
pub fn env_var(key: &str) -> String {
    let mut buf = String::with_capacity(ENV_PREFIX.len() + key.len());
    buf.push_str(ENV_PREFIX);
    buf.push_str(key);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_dot_dir_is_cowd() {
        // Only test default when env var is not set
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
        let cwd = std::path::Path::new("/tmp/workspace");
        if std::env::var("COWD_DIR_NAME").is_err() {
            assert_eq!(project_dot_dir(cwd), std::path::PathBuf::from("/tmp/workspace/.cowd"));
        }
    }
}
