use std::env;
use std::io;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

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
    let cwd = resolve_cwd(input.cwd.as_deref())?;
    if input.run_in_background.unwrap_or(false) {
        let child = prepare_command(&input.command, &cwd, false)
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
            backgrounded_by_user: Some(false),
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

    execute_bash_sync(input, cwd)
}

fn resolve_cwd(cwd: Option<&str>) -> io::Result<PathBuf> {
    match cwd {
        Some(cwd) => {
            let path = PathBuf::from(cwd);
            if path.is_absolute() {
                Ok(path)
            } else {
                Ok(env::current_dir()?.join(path))
            }
        }
        None => env::current_dir(),
    }
}

fn execute_bash_sync(
    input: BashCommandInput,
    cwd: std::path::PathBuf,
) -> io::Result<BashCommandOutput> {
    let mut command = prepare_command(&input.command, &cwd, false);
    let output = if let Some(timeout_ms) = input.timeout {
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

fn prepare_command(command: &str, cwd: &std::path::Path, create_dirs: bool) -> Command {
    if create_dirs {
        let _ = std::fs::create_dir_all(cwd);
    }
    let mut cmd = Command::new("sh");
    cmd.arg("-lc").arg(command).current_dir(cwd);
    cmd
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
