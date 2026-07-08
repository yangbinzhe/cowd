use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionRunnerResult {
    pub run_id: String,
    pub candidate_id: String,
    pub mode: String,
    pub command: String,
    pub exit_code: i32,
    pub duration_ms: u128,
    pub stdout_summary: String,
    pub stderr_summary: String,
    pub stdout_log_path: String,
    pub stderr_log_path: String,
    pub artifact_paths: Vec<String>,
    pub changed_files: Vec<String>,
    pub mainline_modified: bool,
    pub policy_violations: Vec<String>,
    pub heartbeat_events: Vec<String>,
}
