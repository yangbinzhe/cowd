use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::Instant,
};

use uuid::Uuid;

use super::{
    candidate::EvolutionCandidate, runner_policy::EvolutionRunnerPolicy,
    runner_result::EvolutionRunnerResult,
};

#[derive(Debug, Clone)]
pub struct IsolatedRunner {
    root: PathBuf,
    policy: EvolutionRunnerPolicy,
}

impl IsolatedRunner {
    #[must_use]
    pub fn new(root: impl AsRef<Path>, policy: EvolutionRunnerPolicy) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            policy,
        }
    }

    pub fn run_artifact_check(
        &self,
        candidate: &EvolutionCandidate,
    ) -> Result<EvolutionRunnerResult, String> {
        let started = Instant::now();
        let run_root = self.root.join(&candidate.candidate_id).join("runs");
        fs::create_dir_all(&run_root).map_err(|error| error.to_string())?;
        let stdout = run_root.join("artifact-check.stdout.log");
        let stderr = run_root.join("artifact-check.stderr.log");
        let artifact_paths = candidate
            .generated_artifacts
            .iter()
            .map(|artifact| artifact.path.clone())
            .chain(candidate.artifact_path.clone())
            .collect::<Vec<_>>();
        let missing = artifact_paths
            .iter()
            .filter(|path| !Path::new(path).exists())
            .cloned()
            .collect::<Vec<_>>();
        fs::write(
            &stdout,
            format!("checked {} artifact refs\n", artifact_paths.len()),
        )
        .map_err(|error| error.to_string())?;
        fs::write(&stderr, missing.join("\n")).map_err(|error| error.to_string())?;
        Ok(EvolutionRunnerResult {
            run_id: format!("evo-run-{}", Uuid::new_v4()),
            candidate_id: candidate.candidate_id.clone(),
            mode: "artifact".to_string(),
            command: "artifact-check".to_string(),
            exit_code: i32::from(!missing.is_empty()),
            duration_ms: started.elapsed().as_millis(),
            stdout_summary: "artifact references checked".to_string(),
            stderr_summary: if missing.is_empty() {
                String::new()
            } else {
                format!("missing artifacts: {}", missing.len())
            },
            stdout_log_path: stdout.display().to_string(),
            stderr_log_path: stderr.display().to_string(),
            artifact_paths,
            changed_files: Vec::new(),
            mainline_modified: false,
            policy_violations: Vec::new(),
            heartbeat_events: vec!["artifact_check_completed".to_string()],
        })
    }

    pub fn run_command(
        &self,
        candidate: &EvolutionCandidate,
        command: &str,
    ) -> Result<EvolutionRunnerResult, String> {
        let started = Instant::now();
        let run_root = self.root.join(&candidate.candidate_id).join("runs");
        fs::create_dir_all(&run_root).map_err(|error| error.to_string())?;
        let stdout = run_root.join("command.stdout.log");
        let stderr = run_root.join("command.stderr.log");
        let mut violations = Vec::new();
        if !self.policy.allows_command(command) {
            violations.push(format!(
                "command blocked by EvolutionRunnerPolicy: {command}"
            ));
            fs::write(&stdout, "").map_err(|error| error.to_string())?;
            fs::write(&stderr, violations.join("\n")).map_err(|error| error.to_string())?;
            return Ok(EvolutionRunnerResult {
                run_id: format!("evo-run-{}", Uuid::new_v4()),
                candidate_id: candidate.candidate_id.clone(),
                mode: "command".to_string(),
                command: command.to_string(),
                exit_code: 126,
                duration_ms: started.elapsed().as_millis(),
                stdout_summary: String::new(),
                stderr_summary: "policy violation".to_string(),
                stdout_log_path: stdout.display().to_string(),
                stderr_log_path: stderr.display().to_string(),
                artifact_paths: Vec::new(),
                changed_files: Vec::new(),
                mainline_modified: false,
                policy_violations: violations,
                heartbeat_events: vec!["command_rejected".to_string()],
            });
        }
        let output = Command::new("sh")
            .arg("-c")
            .arg(command)
            .output()
            .map_err(|error| error.to_string())?;
        fs::write(&stdout, &output.stdout).map_err(|error| error.to_string())?;
        fs::write(&stderr, &output.stderr).map_err(|error| error.to_string())?;
        Ok(EvolutionRunnerResult {
            run_id: format!("evo-run-{}", Uuid::new_v4()),
            candidate_id: candidate.candidate_id.clone(),
            mode: "command".to_string(),
            command: command.to_string(),
            exit_code: output.status.code().unwrap_or(1),
            duration_ms: started.elapsed().as_millis(),
            stdout_summary: summarize(&output.stdout),
            stderr_summary: summarize(&output.stderr),
            stdout_log_path: stdout.display().to_string(),
            stderr_log_path: stderr.display().to_string(),
            artifact_paths: Vec::new(),
            changed_files: Vec::new(),
            mainline_modified: false,
            policy_violations: Vec::new(),
            heartbeat_events: vec!["command_completed".to_string()],
        })
    }
}

fn summarize(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .lines()
        .take(8)
        .collect::<Vec<_>>()
        .join("\n")
}
