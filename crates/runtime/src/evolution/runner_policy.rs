use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionRunnerPolicy {
    pub allowed_roots: Vec<String>,
    pub allowed_commands: Vec<String>,
    pub blocked_commands: Vec<String>,
    pub env_policy: String,
    pub network_policy: String,
    pub timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub artifact_limit_bytes: u64,
    pub log_limit_bytes: u64,
    pub cleanup_policy: String,
    pub dirty_tree_policy: String,
    pub mainline_write_detection: bool,
}

impl Default for EvolutionRunnerPolicy {
    fn default() -> Self {
        Self {
            allowed_roots: vec!["target/evolution".to_string(), "/tmp".to_string()],
            allowed_commands: vec![
                "cargo metadata".to_string(),
                "cargo check".to_string(),
                "cargo test".to_string(),
                "cargo fmt".to_string(),
                "npm test".to_string(),
            ],
            blocked_commands: vec![
                "rm -rf".to_string(),
                "git reset --hard".to_string(),
                "git checkout --".to_string(),
                "mkfs".to_string(),
                "shutdown".to_string(),
            ],
            env_policy: "minimal".to_string(),
            network_policy: "disabled_by_default".to_string(),
            timeout_ms: 300_000,
            heartbeat_interval_ms: 5_000,
            artifact_limit_bytes: 25 * 1024 * 1024,
            log_limit_bytes: 2 * 1024 * 1024,
            cleanup_policy: "retain_on_failure".to_string(),
            dirty_tree_policy: "detect_and_fail".to_string(),
            mainline_write_detection: true,
        }
    }
}

impl EvolutionRunnerPolicy {
    #[must_use]
    pub fn allows_command(&self, command: &str) -> bool {
        let command = command.trim();
        if command.is_empty() || command == "true" {
            return false;
        }
        !self
            .blocked_commands
            .iter()
            .any(|blocked| command.contains(blocked))
            && self
                .allowed_commands
                .iter()
                .any(|allowed| command == allowed || command.starts_with(&format!("{allowed} ")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_rejects_empty_true_and_destructive_commands() {
        let policy = EvolutionRunnerPolicy::default();
        assert!(!policy.allows_command(""));
        assert!(!policy.allows_command("true"));
        assert!(!policy.allows_command("cargo check && git reset --hard"));
        assert!(policy.allows_command("cargo metadata --format-version 1 --no-deps"));
    }
}
