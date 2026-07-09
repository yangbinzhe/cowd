use std::{
    collections::BTreeSet,
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
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let problems = validate_artifacts(candidate, &artifact_paths);
        fs::write(
            &stdout,
            format!(
                "checked {} artifact refs for candidate {}\n",
                artifact_paths.len(),
                candidate.candidate_id
            ),
        )
        .map_err(|error| error.to_string())?;
        fs::write(&stderr, problems.join("\n")).map_err(|error| error.to_string())?;
        Ok(EvolutionRunnerResult {
            run_id: format!("evo-run-{}", Uuid::new_v4()),
            candidate_id: candidate.candidate_id.clone(),
            mode: "artifact".to_string(),
            command: "artifact-check".to_string(),
            exit_code: i32::from(!problems.is_empty()),
            duration_ms: started.elapsed().as_millis(),
            stdout_summary: "artifact references checked".to_string(),
            stderr_summary: if problems.is_empty() {
                String::new()
            } else {
                format!("artifact validation failures: {}", problems.len())
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
        self.run_named_command(candidate, "command", command)
    }

    pub fn run_named_command(
        &self,
        candidate: &EvolutionCandidate,
        mode: &str,
        command: &str,
    ) -> Result<EvolutionRunnerResult, String> {
        let started = Instant::now();
        let run_root = self.root.join(&candidate.candidate_id).join("runs");
        fs::create_dir_all(&run_root).map_err(|error| error.to_string())?;
        let mode = safe_mode(mode);
        let stdout = run_root.join(format!("{mode}.stdout.log"));
        let stderr = run_root.join(format!("{mode}.stderr.log"));
        let command = command.trim();
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
                mode,
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
            mode,
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

fn validate_artifacts(candidate: &EvolutionCandidate, artifact_paths: &[String]) -> Vec<String> {
    let mut problems = Vec::new();
    if artifact_paths.is_empty() {
        problems.push("candidate generated no artifact references".to_string());
        return problems;
    }
    for path in artifact_paths {
        let artifact_path = Path::new(path);
        if !artifact_path.exists() {
            problems.push(format!("missing artifact: {path}"));
            continue;
        }
        match fs::metadata(artifact_path) {
            Ok(metadata) if metadata.len() == 0 => {
                problems.push(format!("empty artifact: {path}"));
                continue;
            }
            Err(error) => {
                problems.push(format!("artifact metadata failed: {path}: {error}"));
                continue;
            }
            _ => {}
        }
        let content = match fs::read_to_string(artifact_path) {
            Ok(content) => content,
            Err(error) => {
                problems.push(format!("artifact read failed: {path}: {error}"));
                continue;
            }
        };
        if !content.contains(&candidate.candidate_id) {
            problems.push(format!("artifact does not reference candidate id: {path}"));
        }
        if path.ends_with(".json") {
            match serde_json::from_str::<serde_json::Value>(&content) {
                Ok(value) => {
                    if value
                        .get("candidate_id")
                        .and_then(|candidate_id| candidate_id.as_str())
                        != Some(candidate.candidate_id.as_str())
                    {
                        problems.push(format!("json artifact candidate_id mismatch: {path}"));
                    }
                }
                Err(error) => problems.push(format!("invalid json artifact: {path}: {error}")),
            }
        }
    }
    problems
}

fn safe_mode(mode: &str) -> String {
    let sanitized = mode
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.trim().is_empty() {
        "command".to_string()
    } else {
        sanitized
    }
}

fn summarize(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .lines()
        .take(8)
        .collect::<Vec<_>>()
        .join("\n")
}
