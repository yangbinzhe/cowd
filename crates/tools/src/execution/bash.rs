use std::env;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use sandbox_launcher::{shell_command, SandboxLaunchSpec};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BashCommandInput {
    pub command: String,
    pub cwd: Option<String>,
    pub timeout: Option<u64>,
    pub description: Option<String>,
    #[serde(rename = "run_in_background")]
    pub run_in_background: Option<bool>,
    #[serde(rename = "dangerouslyDisableSandbox")]
    pub dangerously_disable_sandbox: Option<bool>,
    #[serde(rename = "namespaceRestrictions")]
    pub namespace_restrictions: Option<bool>,
    #[serde(rename = "isolateNetwork")]
    pub isolate_network: Option<bool>,
    #[serde(rename = "filesystemMode")]
    pub filesystem_mode: Option<String>,
    #[serde(rename = "allowedMounts")]
    pub allowed_mounts: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BashCommandOutput {
    pub stdout: String,
    pub stderr: String,
    #[serde(rename = "rawOutputPath")]
    pub raw_output_path: Option<String>,
    pub interrupted: bool,
    #[serde(rename = "isImage")]
    pub is_image: Option<bool>,
    #[serde(rename = "backgroundTaskId")]
    pub background_task_id: Option<String>,
    #[serde(rename = "backgroundedByUser")]
    pub backgrounded_by_user: Option<bool>,
    #[serde(rename = "assistantAutoBackgrounded")]
    pub assistant_auto_backgrounded: Option<bool>,
    #[serde(rename = "dangerouslyDisableSandbox")]
    pub dangerously_disable_sandbox: Option<bool>,
    #[serde(rename = "returnCodeInterpretation")]
    pub return_code_interpretation: Option<String>,
    #[serde(rename = "noOutputExpected")]
    pub no_output_expected: Option<bool>,
    #[serde(rename = "structuredContent")]
    pub structured_content: Option<Vec<serde_json::Value>>,
    #[serde(rename = "persistedOutputPath")]
    pub persisted_output_path: Option<String>,
    #[serde(rename = "persistedOutputSize")]
    pub persisted_output_size: Option<u64>,
    #[serde(rename = "sandboxStatus")]
    pub sandbox_status: Option<serde_json::Value>,
}

pub fn execute_bash(input: BashCommandInput) -> io::Result<BashCommandOutput> {
    let workspace = env::current_dir()?;
    execute_bash_in_workspace(input, workspace)
}

pub fn execute_bash_in_workspace(
    input: BashCommandInput,
    workspace_root: impl AsRef<Path>,
) -> io::Result<BashCommandOutput> {
    let workspace_root = workspace_root.as_ref().canonicalize()?;
    let cwd = resolve_cwd(input.cwd.as_deref(), &workspace_root)?;
    if input.run_in_background.unwrap_or(false) {
        let child = prepare_command(&input.command, &workspace_root, &cwd, false)?
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        return Ok(BashCommandOutput {
            stdout: String::new(),
            stderr: String::new(),
            raw_output_path: None,
            interrupted: false,
            is_image: None,
            background_task_id: Some(child.id().to_string()),
            backgrounded_by_user: Some(true),
            assistant_auto_backgrounded: Some(false),
            dangerously_disable_sandbox: input.dangerously_disable_sandbox,
            return_code_interpretation: None,
            no_output_expected: Some(true),
            structured_content: None,
            persisted_output_path: None,
            persisted_output_size: None,
            sandbox_status: Some(serde_json::json!({"mode":"tools-local"})),
        });
    }

    execute_bash_sync(input, workspace_root, cwd)
}

fn resolve_cwd(cwd: Option<&str>, workspace_root: &Path) -> io::Result<PathBuf> {
    match cwd {
        Some(cwd) => {
            let path = PathBuf::from(cwd);
            let resolved = if path.is_absolute() {
                path
            } else {
                workspace_root.join(path)
            }
            .canonicalize()?;
            if resolved.starts_with(workspace_root) {
                Ok(resolved)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "bash cwd must remain inside the leased workspace",
                ))
            }
        }
        None => Ok(workspace_root.to_path_buf()),
    }
}

fn execute_bash_sync(
    input: BashCommandInput,
    workspace_root: PathBuf,
    cwd: std::path::PathBuf,
) -> io::Result<BashCommandOutput> {
    let mut command = prepare_command(&input.command, &workspace_root, &cwd, false)?;
    let output = if let Some(timeout_ms) = input.timeout {
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = command.spawn()?;
        let started = std::time::Instant::now();
        loop {
            if child.try_wait()?.is_some() {
                break (child.wait_with_output()?, false);
            }
            if started.elapsed() >= Duration::from_millis(timeout_ms) {
                let _ = child.kill();
                let output = child.wait_with_output()?;
                return Ok(BashCommandOutput {
                    stdout: String::new(),
                    stderr: if output.stderr.is_empty() {
                        format!("Command exceeded timeout of {timeout_ms} ms")
                    } else {
                        format!(
                            "{}\nCommand exceeded timeout of {timeout_ms} ms",
                            String::from_utf8_lossy(&output.stderr).trim_end()
                        )
                    },
                    raw_output_path: None,
                    interrupted: true,
                    is_image: None,
                    background_task_id: None,
                    backgrounded_by_user: None,
                    assistant_auto_backgrounded: None,
                    dangerously_disable_sandbox: input.dangerously_disable_sandbox,
                    return_code_interpretation: Some(String::from("timeout")),
                    no_output_expected: Some(true),
                    structured_content: None,
                    persisted_output_path: None,
                    persisted_output_size: None,
                    sandbox_status: Some(serde_json::json!({"mode":"tools-local"})),
                });
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    } else {
        (command.output()?, false)
    };

    let (output, interrupted) = output;
    let stdout = truncate_output(&String::from_utf8_lossy(&output.stdout));
    let stderr = truncate_output(&String::from_utf8_lossy(&output.stderr));
    Ok(BashCommandOutput {
        stdout: stdout.clone(),
        stderr: stderr.clone(),
        raw_output_path: None,
        interrupted,
        is_image: None,
        background_task_id: None,
        backgrounded_by_user: None,
        assistant_auto_backgrounded: None,
        dangerously_disable_sandbox: input.dangerously_disable_sandbox,
        return_code_interpretation: output
            .status
            .code()
            .filter(|code| *code != 0)
            .map(|code| format!("exit_code:{code}")),
        no_output_expected: Some(stdout.trim().is_empty() && stderr.trim().is_empty()),
        structured_content: None,
        persisted_output_path: None,
        persisted_output_size: None,
        sandbox_status: Some(serde_json::json!({"mode":"tools-local"})),
    })
}

fn prepare_command(
    command: &str,
    workspace_root: &Path,
    cwd: &Path,
    create_dirs: bool,
) -> io::Result<Command> {
    if create_dirs {
        let _ = std::fs::create_dir_all(cwd);
    }
    let mut spec = SandboxLaunchSpec::workspace(workspace_root.to_path_buf());
    spec.working_directory = Some(cwd.to_path_buf());
    shell_command(command, &spec)
        .map(|prepared| prepared.into_command())
        .map_err(|error| io::Error::new(io::ErrorKind::PermissionDenied, error.to_string()))
}

fn truncate_output(value: &str) -> String {
    const MAX_OUTPUT: usize = 200_000;
    if value.len() <= MAX_OUTPUT {
        return value.to_string();
    }
    format!(
        "{}\n\n[output truncated: {} bytes total]",
        &value[..MAX_OUTPUT],
        value.len()
    )
}
