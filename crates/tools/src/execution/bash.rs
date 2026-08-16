#[cfg(test)]
use std::env;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sandbox_launcher::{shell_command, SandboxLaunchSpec, SandboxWorkspaceAccess};
use serde::{Deserialize, Serialize};

/// Bounded model-visible head/tail windows for stdout and stderr. Keeping
/// both edges (instead of only a prefix) preserves the beginning of a build
/// log and its final error lines, which are the two diagnostically useful
/// parts of a long command.
pub const BASH_RETURN_HEAD_BYTES: usize = 64 * 1024;
pub const BASH_RETURN_TAIL_BYTES: usize = 64 * 1024;
/// Total captured bytes after which the full output is persisted as an
/// artifact instead of being returned inline.
pub const BASH_PERSIST_THRESHOLD_BYTES: u64 = 128 * 1024;
/// Hard ceiling for draining pipes after the child process exits. This
/// protects against grandchildren that keep a pipe open indefinitely.
pub const BASH_IO_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
/// Minimum interval between progress samples.
const BASH_PROGRESS_INTERVAL: Duration = Duration::from_millis(250);

/// Effective head/tail/persist limits (P11). Defaults stay the documented
/// contract; operators may override per deployment via environment variables.
fn effective_head_bytes() -> usize {
    env_limit_bytes("COWD_BASH_HEAD_BYTES", BASH_RETURN_HEAD_BYTES)
}

fn effective_tail_bytes() -> usize {
    env_limit_bytes("COWD_BASH_TAIL_BYTES", BASH_RETURN_TAIL_BYTES)
}

fn effective_persist_threshold() -> u64 {
    std::env::var("COWD_BASH_PERSIST_THRESHOLD_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| (1024..=16 * 1024 * 1024).contains(value))
        .unwrap_or(BASH_PERSIST_THRESHOLD_BYTES)
}

fn effective_io_drain_timeout() -> Duration {
    env_duration_ms("COWD_BASH_IO_DRAIN_MS", BASH_IO_DRAIN_TIMEOUT)
}

fn effective_progress_interval() -> Duration {
    env_duration_ms("COWD_BASH_PROGRESS_MS", BASH_PROGRESS_INTERVAL)
}

fn env_limit_bytes(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| (1024..=8 * 1024 * 1024).contains(value))
        .unwrap_or(default)
}

fn env_duration_ms(key: &str, default: Duration) -> Duration {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| (10..=300_000).contains(value))
        .map(Duration::from_millis)
        .unwrap_or(default)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShellEnvironmentEntry {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ShellInheritMode {
    /// Inherit only the safe locale/proxy allowlist plus explicit
    /// `include_only` and `set` entries.
    #[default]
    Safe,
    /// Inherit the full host environment, still dropping `COWD_*` control
    /// variables and secret-looking keys unless explicitly included.
    All,
    /// Inherit nothing from the host; only `set` entries are provided.
    None,
}

/// Runtime-owned workspace mount authority. The model-facing compiler strips
/// this field; Runtime reinserts it from the immutable Session policy after
/// authorization, so network isolation can never be used as a proxy.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum BashWorkspaceAccess {
    ReadOnly,
    #[default]
    ReadWrite,
}

/// Shell environment policy (T5). The default is `inherit: safe`, which masks
/// secrets and control-plane variables even when `inherit: all` is requested.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ShellEnvironmentPolicy {
    #[serde(default)]
    pub inherit: ShellInheritMode,
    /// Explicit host keys to include even when they look secret-like.
    #[serde(default)]
    pub include_only: Vec<String>,
    /// Host keys to drop regardless of mode.
    #[serde(default)]
    pub exclude: Vec<String>,
    /// Explicit key/value pairs passed into the sandbox. These override any
    /// inherited value with the same key.
    #[serde(default)]
    pub set: Vec<ShellEnvironmentEntry>,
}

/// One bounded progress observation emitted while a bash command is running.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BashProgressSample {
    #[serde(rename = "atMs")]
    pub at_ms: u64,
    #[serde(rename = "stdoutBytes")]
    pub stdout_bytes: u64,
    #[serde(rename = "stderrBytes")]
    pub stderr_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BashCommandInput {
    pub command: String,
    pub cwd: Option<String>,
    pub timeout_ms: Option<u64>,
    pub description: Option<String>,
    #[serde(rename = "run_in_background")]
    pub run_in_background: Option<bool>,
    #[serde(rename = "dangerouslyDisableSandbox")]
    pub dangerously_disable_sandbox: Option<bool>,
    #[serde(rename = "isolateNetwork")]
    pub isolate_network: Option<bool>,
    #[serde(rename = "workspaceAccess")]
    pub workspace_access: Option<BashWorkspaceAccess>,
    #[serde(rename = "allowedMounts")]
    pub allowed_mounts: Option<Vec<String>>,
    /// Shell environment policy. Absent means `inherit: safe`, which is the
    /// secret-masking default.
    #[serde(default)]
    pub env: Option<ShellEnvironmentPolicy>,
}

const MAX_TOOL_TIMEOUT_MS: u64 = 300_000;

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
    /// True when stdout or stderr exceeded the bounded head/tail windows.
    #[serde(default, rename = "returnTruncated")]
    pub return_truncated: bool,
    /// Bounded progress observations captured while the command ran.
    #[serde(default)]
    pub progress: Vec<BashProgressSample>,
}

impl BashCommandOutput {
    fn structured_sandbox_status(input: &BashCommandInput) -> serde_json::Value {
        let disable_sandbox = input.dangerously_disable_sandbox.unwrap_or(false);
        let isolate_network = input.isolate_network.unwrap_or(false);
        let posture = if disable_sandbox {
            "host-full-access"
        } else if input.workspace_access == Some(BashWorkspaceAccess::ReadOnly) {
            "read-only-sandbox"
        } else {
            "workspace-write-sandbox"
        };
        serde_json::json!({
            "posture": posture,
            "enabled": !disable_sandbox,
            "network_enabled": !isolate_network,
            "kernel_hardening_required": !disable_sandbox,
            "fallback_reason": serde_json::Value::Null,
        })
    }

    fn blank(
        input: &BashCommandInput,
        interrupted: bool,
        return_code_interpretation: Option<String>,
    ) -> Self {
        Self {
            stdout: String::new(),
            stderr: String::new(),
            raw_output_path: None,
            interrupted,
            is_image: None,
            background_task_id: None,
            backgrounded_by_user: None,
            assistant_auto_backgrounded: None,
            dangerously_disable_sandbox: input.dangerously_disable_sandbox,
            return_code_interpretation,
            no_output_expected: Some(true),
            structured_content: None,
            persisted_output_path: None,
            persisted_output_size: None,
            sandbox_status: Some(Self::structured_sandbox_status(input)),
            return_truncated: false,
            progress: Vec::new(),
        }
    }
}

#[cfg(test)]
pub fn execute_bash(input: BashCommandInput) -> io::Result<BashCommandOutput> {
    let workspace = env::current_dir()?;
    execute_bash_in_workspace(input, workspace)
}

pub fn execute_bash_in_workspace(
    input: BashCommandInput,
    workspace_root: impl AsRef<Path>,
) -> io::Result<BashCommandOutput> {
    reject_model_escalation_fields(&input).map_err(io::Error::other)?;
    let workspace_root = workspace_root.as_ref().canonicalize()?;
    let cwd = resolve_cwd(input.cwd.as_deref(), &workspace_root)?;
    if input.run_in_background.unwrap_or(false) {
        let mut prepared = prepare_command(&input, &workspace_root, &cwd, false)?;
        let child = prepared
            .command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        return Ok(BashCommandOutput {
            background_task_id: Some(child.id().to_string()),
            backgrounded_by_user: Some(true),
            assistant_auto_backgrounded: Some(false),
            sandbox_status: Some(prepared.sandbox_status),
            ..BashCommandOutput::blank(&input, false, None)
        });
    }
    execute_bash_sync(input, workspace_root, cwd)
}

/// S-01/S-02/S-03 fail-closed gate: mount roots, host-secret inheritance and
/// background PID-only execution are capability grants, not model parameters.
/// Operator-configured sandbox mounts stay available through
/// `SandboxLaunchSpec`/Runtime config and never enter this generic shell input.
pub fn reject_model_escalation_fields(input: &BashCommandInput) -> Result<(), String> {
    if input
        .allowed_mounts
        .as_ref()
        .is_some_and(|mounts| !mounts.is_empty())
    {
        return Err(
            "bash allowedMounts is not model-controllable; mounts are granted by operator sandbox configuration only (S-01)"
                .to_string(),
        );
    }
    if let Some(policy) = input.env.as_ref() {
        if !policy.include_only.is_empty() {
            return Err(
                "bash env.include_only is not model-controllable; host secrets cannot be requested from a generic shell (S-02)"
                    .to_string(),
            );
        }
        if policy.inherit == ShellInheritMode::All {
            return Err(
                "bash env.inherit=all is not model-controllable; use inherit=safe or none (S-02)"
                    .to_string(),
            );
        }
    }
    if input.run_in_background.unwrap_or(false) {
        return Err(
            "bash run_in_background is not exposed as a model capability until durable lifecycle management lands (S-03)"
                .to_string(),
        );
    }
    Ok(())
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
    cwd: PathBuf,
) -> io::Result<BashCommandOutput> {
    let prepared = prepare_command(&input, &workspace_root, &cwd, false)?;
    let mut command = prepared.command;
    let sandbox_status = prepared.sandbox_status;
    let (output, interrupted) = if let Some(timeout_ms) = input.timeout_ms {
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = command.spawn()?;
        let started = Instant::now();
        loop {
            if child.try_wait()?.is_some() {
                break (child.wait_with_output()?, false);
            }
            if started.elapsed() >= Duration::from_millis(timeout_ms) {
                if !kill_process_group(Some(child.id())) {
                    let _ = child.kill();
                }
                let output = child.wait_with_output()?;
                return Ok(BashCommandOutput {
                    stderr: if output.stderr.is_empty() {
                        format!("Command exceeded timeout of {timeout_ms} ms")
                    } else {
                        format!(
                            "{}\nCommand exceeded timeout of {timeout_ms} ms",
                            String::from_utf8_lossy(&output.stderr).trim_end()
                        )
                    },
                    interrupted: true,
                    return_code_interpretation: Some(String::from("timeout")),
                    no_output_expected: Some(true),
                    sandbox_status: Some(complete_sandbox_status(
                        sandbox_status,
                        output.status.code(),
                        true,
                    )),
                    ..BashCommandOutput::blank(&input, true, None)
                });
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    } else {
        (command.output()?, false)
    };

    let captured = CapturedOutput::from_bytes(&output.stdout, &output.stderr, None);
    Ok(build_output(
        &input,
        captured,
        interrupted,
        complete_sandbox_status(sandbox_status, output.status.code(), interrupted),
        output
            .status
            .code()
            .filter(|code| *code != 0)
            .map(|code| format!("exit_code:{code}")),
    ))
}

/// Tokio-native bash execution (T4). This path shares validation and lease
/// checks with the blocking fallback through `ToolHostLease::execute_async`;
/// it only replaces the process runtime: bounded head/tail capture, process
/// group kill, a 2s IO drain ceiling, and progress samples.
pub async fn execute_bash_async_in_workspace(
    input: BashCommandInput,
    workspace_root: impl AsRef<Path>,
    progress: Option<Arc<dyn Fn(BashProgressSample) + Send + Sync>>,
) -> io::Result<BashCommandOutput> {
    let workspace_root = workspace_root.as_ref().canonicalize()?;
    let cwd = resolve_cwd(input.cwd.as_deref(), &workspace_root)?;
    if input.run_in_background.unwrap_or(false) {
        let mut prepared = prepare_command(&input, &workspace_root, &cwd, false)?;
        let child = prepared
            .command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        return Ok(BashCommandOutput {
            background_task_id: Some(child.id().to_string()),
            backgrounded_by_user: Some(true),
            assistant_auto_backgrounded: Some(false),
            sandbox_status: Some(prepared.sandbox_status),
            ..BashCommandOutput::blank(&input, false, None)
        });
    }

    let prepared = prepare_command(&input, &workspace_root, &cwd, false)?;
    let sandbox_status = prepared.sandbox_status;
    let mut command = tokio::process::Command::from(prepared.command);
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    #[cfg(unix)]
    {
        command.process_group(0);
    }
    let mut child = command.spawn()?;
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let shared = Arc::new(Mutex::new(BashProgressState::new()));
    let progress_stdout = progress.clone();
    let progress_stderr = progress.clone();
    let out_task = tokio::spawn(capture_async_stream(
        stdout,
        Arc::clone(&shared),
        StreamKind::Stdout,
        progress_stdout,
    ));
    let err_task = tokio::spawn(capture_async_stream(
        stderr,
        Arc::clone(&shared),
        StreamKind::Stderr,
        progress_stderr,
    ));

    let (status, interrupted) = if let Some(timeout_ms) = input.timeout_ms {
        match tokio::time::timeout(Duration::from_millis(timeout_ms), child.wait()).await {
            Ok(status) => (status?, false),
            Err(_) => {
                if !kill_process_group(child.id()) {
                    let _ = child.kill().await;
                }
                (child.wait().await?, true)
            }
        }
    } else {
        (child.wait().await?, false)
    };

    // Drain pipes for at most 2s after exit; a descendant holding a pipe open
    // must not stall the turn forever.
    let drain_timeout = effective_io_drain_timeout();
    let out_result = tokio::time::timeout(drain_timeout, out_task).await;
    let err_result = tokio::time::timeout(drain_timeout, err_task).await;
    let out: CapturedStream = match out_result {
        Ok(Ok(Ok(captured))) => captured,
        Ok(Ok(Err(error))) => return Err(error),
        Ok(Err(error)) => return Err(io::Error::other(error.to_string())),
        Err(_) => return Err(io::Error::other("bash stdout drain exceeded 2s")),
    };
    let err: CapturedStream = match err_result {
        Ok(Ok(Ok(captured))) => captured,
        Ok(Ok(Err(error))) => return Err(error),
        Ok(Err(error)) => return Err(io::Error::other(error.to_string())),
        Err(_) => return Err(io::Error::other("bash stderr drain exceeded 2s")),
    };

    let samples = shared
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .samples
        .clone();
    let captured = CapturedOutput::combine(out, err, samples);
    let output = build_output(
        &input,
        captured,
        interrupted,
        complete_sandbox_status(sandbox_status, status.code(), interrupted),
        status
            .code()
            .filter(|code| *code != 0)
            .map(|code| format!("exit_code:{code}")),
    );
    if interrupted {
        // The timeout path still returns structured output; preserve it while
        // keeping the existing contract's stderr note.
        return Ok(output);
    }
    Ok(output)
}

#[derive(Debug, Clone, Copy)]
enum StreamKind {
    Stdout,
    Stderr,
}

struct BashProgressState {
    started: Instant,
    stdout_bytes: u64,
    stderr_bytes: u64,
    last_emit_at: Option<Instant>,
    samples: Vec<BashProgressSample>,
}

impl BashProgressState {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            stdout_bytes: 0,
            stderr_bytes: 0,
            last_emit_at: None,
            samples: Vec::new(),
        }
    }

    fn observe(
        &mut self,
        kind: StreamKind,
        bytes: u64,
        callback: Option<&Arc<dyn Fn(BashProgressSample) + Send + Sync>>,
    ) {
        match kind {
            StreamKind::Stdout => self.stdout_bytes = self.stdout_bytes.saturating_add(bytes),
            StreamKind::Stderr => self.stderr_bytes = self.stderr_bytes.saturating_add(bytes),
        }
        let now = Instant::now();
        if self
            .last_emit_at
            .is_some_and(|last| now.duration_since(last) < effective_progress_interval())
        {
            return;
        }
        self.last_emit_at = Some(now);
        let sample = BashProgressSample {
            at_ms: self.started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            stdout_bytes: self.stdout_bytes,
            stderr_bytes: self.stderr_bytes,
        };
        self.samples.push(sample.clone());
        if let Some(callback) = callback {
            callback(sample);
        }
    }
}

struct CapturedStream {
    head_limit: usize,
    tail_limit: usize,
    persist_threshold: u64,
    total_bytes: u64,
    truncated: bool,
    head: Vec<u8>,
    tail: Vec<u8>,
    artifact: Option<(PathBuf, std::fs::File)>,
    artifact_bytes: u64,
}

impl CapturedStream {
    fn new() -> Self {
        Self {
            head_limit: effective_head_bytes(),
            tail_limit: effective_tail_bytes(),
            persist_threshold: effective_persist_threshold(),
            total_bytes: 0,
            truncated: false,
            head: Vec::with_capacity(effective_head_bytes().min(8192)),
            tail: Vec::with_capacity(effective_tail_bytes().min(8192)),
            artifact: None,
            artifact_bytes: 0,
        }
    }

    fn push(&mut self, chunk: &[u8]) {
        if chunk.is_empty() {
            return;
        }
        self.total_bytes = self.total_bytes.saturating_add(chunk.len() as u64);
        if self.artifact.is_none() && self.total_bytes > self.persist_threshold {
            if let Ok(path) = artifact_path_for() {
                if let Ok(file) = std::fs::File::create(&path) {
                    let mut artifact = Some((path, file));
                    if let Some((_, file)) = artifact.as_mut() {
                        let _ = write_all_std(file, &self.head);
                        if self.truncated {
                            let _ = write_all_std(file, &self.tail);
                        }
                        self.artifact_bytes = (self.head.len() + self.tail.len()) as u64;
                        self.artifact = artifact;
                    }
                }
            }
        }
        if let Some((_, file)) = self.artifact.as_mut() {
            let _ = write_all_std(file, chunk);
            self.artifact_bytes = self.artifact_bytes.saturating_add(chunk.len() as u64);
        }
        if self.head.len() < self.head_limit {
            let remaining = self.head_limit - self.head.len();
            let take = remaining.min(chunk.len());
            self.head.extend_from_slice(&chunk[..take]);
        }
        if chunk.len() >= self.tail_limit {
            self.tail.clear();
            self.tail
                .extend_from_slice(&chunk[chunk.len() - self.tail_limit..]);
            self.truncated = true;
        } else {
            let keep = self.tail_limit - chunk.len();
            if self.tail.len() > keep {
                self.tail.drain(..self.tail.len() - keep);
                self.truncated = true;
            }
            self.tail.extend_from_slice(chunk);
        }
    }

    fn finish(mut self) -> (Vec<u8>, Option<PathBuf>, u64, bool) {
        let artifact = self.artifact.take();
        let path = artifact.map(|(path, _)| path);
        let artifact_bytes = self.artifact_bytes;
        let mut rendered = self.head;
        if self.truncated {
            if rendered.len() > 0 && !rendered.ends_with(b"\n") {
                rendered.push(b'\n');
            }
            rendered.extend_from_slice(b"\n[output truncated]\n");
            rendered.extend_from_slice(&self.tail);
        }
        (rendered, path, artifact_bytes, self.truncated)
    }
}

fn write_all_std(mut file: &std::fs::File, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write;
    file.write_all(bytes)
}

fn artifact_path_for() -> io::Result<PathBuf> {
    // P6: artifacts live in a persistent, TTL-managed directory instead of
    // /tmp so model-visible `persistedOutputPath` references stay valid and
    // evidence remains resolvable until the next cleanup cycle.
    let dir = std::env::var_os("COWD_BASH_ARTIFACT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(std::env::temp_dir)
                .join(".cowd")
                .join("storage")
                .join("bash-artifacts")
        });
    std::fs::create_dir_all(&dir)?;
    Ok(dir.join(format!("{}-{}.out", std::process::id(), uuid_v4_short())))
}

fn uuid_v4_short() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{nanos:x}")
}

struct CapturedOutput {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    stdout_artifact: Option<PathBuf>,
    stderr_artifact: Option<PathBuf>,
    stdout_artifact_bytes: u64,
    stderr_artifact_bytes: u64,
    stdout_truncated: bool,
    stderr_truncated: bool,
    samples: Vec<BashProgressSample>,
}

impl CapturedOutput {
    fn from_bytes(stdout: &[u8], stderr: &[u8], samples: Option<Vec<BashProgressSample>>) -> Self {
        let mut out = CapturedStream::new();
        out.push(stdout);
        let mut err = CapturedStream::new();
        err.push(stderr);
        let (stdout, stdout_artifact, stdout_artifact_bytes, stdout_truncated) = out.finish();
        let (stderr, stderr_artifact, stderr_artifact_bytes, stderr_truncated) = err.finish();
        Self {
            stdout,
            stderr,
            stdout_artifact,
            stderr_artifact,
            stdout_artifact_bytes,
            stderr_artifact_bytes,
            stdout_truncated,
            stderr_truncated,
            samples: samples.unwrap_or_default(),
        }
    }

    fn combine(out: CapturedStream, err: CapturedStream, samples: Vec<BashProgressSample>) -> Self {
        let (stdout, stdout_artifact, stdout_artifact_bytes, stdout_truncated) = out.finish();
        let (stderr, stderr_artifact, stderr_artifact_bytes, stderr_truncated) = err.finish();
        Self {
            stdout,
            stderr,
            stdout_artifact,
            stderr_artifact,
            stdout_artifact_bytes,
            stderr_artifact_bytes,
            stdout_truncated,
            stderr_truncated,
            samples,
        }
    }
}

async fn capture_async_stream<R>(
    stream: Option<R>,
    shared: Arc<Mutex<BashProgressState>>,
    kind: StreamKind,
    progress: Option<Arc<dyn Fn(BashProgressSample) + Send + Sync>>,
) -> io::Result<CapturedStream>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    use tokio::io::AsyncReadExt;
    let Some(mut stream) = stream else {
        return Ok(CapturedStream::new());
    };
    let mut captured = CapturedStream::new();
    let mut buffer = vec![0u8; 16 * 1024];
    loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        captured.push(&buffer[..read]);
        let mut state = shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.observe(kind, read as u64, progress.as_ref());
    }
    Ok(captured)
}

fn kill_process_group(child_pid: Option<u32>) -> bool {
    let Some(child_pid) = child_pid else {
        return false;
    };
    #[cfg(unix)]
    {
        let group = format!("-{child_pid}");
        return Command::new("kill")
            .args(["-KILL", "--", &group])
            .status()
            .is_ok_and(|status| status.success())
            || Command::new("kill")
                .args(["-KILL", &child_pid.to_string()])
                .status()
                .is_ok_and(|status| status.success());
    }
    #[cfg(not(unix))]
    {
        let _ = child_pid;
        false
    }
}

struct PreparedBashCommand {
    command: Command,
    sandbox_status: serde_json::Value,
}

fn prepare_command(
    input: &BashCommandInput,
    workspace_root: &Path,
    cwd: &Path,
    create_dirs: bool,
) -> io::Result<PreparedBashCommand> {
    if input
        .timeout_ms
        .is_some_and(|timeout_ms| !(1..=MAX_TOOL_TIMEOUT_MS).contains(&timeout_ms))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("timeout_ms must be between 1 and {MAX_TOOL_TIMEOUT_MS} milliseconds"),
        ));
    }
    if create_dirs {
        let _ = std::fs::create_dir_all(cwd);
    }
    if input.dangerously_disable_sandbox.unwrap_or(false) {
        if input.isolate_network.unwrap_or(false) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "host-full-access cannot claim network isolation; select a sandbox posture",
            ));
        }
        if input.workspace_access != Some(BashWorkspaceAccess::ReadWrite) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "host-full-access requires explicit read_write workspace authority",
            ));
        }
        let mut command = Command::new("/bin/bash");
        command
            .arg("-c")
            .arg(
                "set -eu; [ -d /proc/$$/fd ]; for entry in /proc/$$/fd/*; do fd=${entry##*/}; case \"$fd\" in 0|1|2|*[!0-9]*|'') ;; *) eval \"exec $fd>&-\" ;; esac; done; exec /bin/bash -c \"$1\"",
            )
            .arg("cowd-host-bootstrap")
            .arg(&input.command)
            .current_dir(cwd);
        command.env_clear();
        command.envs(build_environment(input.env.as_ref())?);
        command.env(
            "PATH",
            "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        );
        command.env("TMPDIR", "/tmp");
        return Ok(PreparedBashCommand {
            command,
            sandbox_status: serde_json::json!({
                "requested_posture": "host-full-access",
                "effective_posture": "host-full-access",
                "backend": "host",
                "workspace_access": "read_write",
                "network_enabled": true,
                "kernel_hardening_applied": false,
                "protected_roots_denied": false,
                "inherited_fds_closed": true,
                "environment_allowlist_enforced": true,
                "prepared_at_ms": now_ms(),
                "state": "prepared",
                "fallback_reason": serde_json::Value::Null,
            }),
        });
    }
    let spec = build_sandbox_spec(input, workspace_root, cwd)?;
    shell_command(&input.command, &spec)
        .map(|prepared| {
            let receipt = prepared.launch_receipt();
            let status = serde_json::json!({
                "requested_posture": BashCommandOutput::structured_sandbox_status(input)["posture"],
                "effective_posture": match receipt.workspace_access {
                    SandboxWorkspaceAccess::ReadOnly => "read-only-sandbox",
                    SandboxWorkspaceAccess::ReadWrite => "workspace-write-sandbox",
                },
                "backend": receipt.backend,
                "workspace_access": match receipt.workspace_access {
                    SandboxWorkspaceAccess::ReadOnly => "read_only",
                    SandboxWorkspaceAccess::ReadWrite => "read_write",
                },
                "network_enabled": receipt.network_enabled,
                "kernel_hardening_applied": receipt.kernel_hardening_applied,
                "protected_roots_denied": receipt.protected_roots_denied,
                "inherited_fds_closed": receipt.inherited_fds_closed,
                "environment_allowlist_enforced": receipt.environment_allowlist_enforced,
                "prepared_at_ms": receipt.prepared_at_ms,
                "state": "prepared",
                "fallback_reason": serde_json::Value::Null,
            });
            PreparedBashCommand {
                command: prepared.into_command(),
                sandbox_status: status,
            }
        })
        .map_err(|error| io::Error::new(io::ErrorKind::PermissionDenied, error.to_string()))
}

fn build_sandbox_spec(
    input: &BashCommandInput,
    workspace_root: &Path,
    cwd: &Path,
) -> io::Result<SandboxLaunchSpec> {
    let mut spec = SandboxLaunchSpec::workspace(workspace_root.to_path_buf());
    spec.working_directory = Some(cwd.to_path_buf());
    // T8: wire the real sandbox switches instead of decorative fields.
    spec.require_kernel_hardening = !input.dangerously_disable_sandbox.unwrap_or(false);
    spec.network_enabled = !input.isolate_network.unwrap_or(false);
    spec.workspace_access = match input.workspace_access.unwrap_or_default() {
        BashWorkspaceAccess::ReadOnly => SandboxWorkspaceAccess::ReadOnly,
        BashWorkspaceAccess::ReadWrite => SandboxWorkspaceAccess::ReadWrite,
    };
    for mount in input.allowed_mounts.iter().flatten() {
        let path = PathBuf::from(mount);
        if !path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("bash allowedMounts entry `{mount}` must be an absolute path"),
            ));
        }
        spec.readable_roots.push(path);
    }
    spec.environment = build_environment(input.env.as_ref())?;
    Ok(spec)
}

fn build_environment(policy: Option<&ShellEnvironmentPolicy>) -> io::Result<Vec<(String, String)>> {
    apply_environment_policy(policy.cloned().unwrap_or_default(), std::env::vars())
}

fn apply_environment_policy(
    policy: ShellEnvironmentPolicy,
    host: impl IntoIterator<Item = (String, String)>,
) -> io::Result<Vec<(String, String)>> {
    let include_only = policy
        .include_only
        .iter()
        .map(|key| key.trim().to_string())
        .filter(|key| !key.is_empty())
        .collect::<std::collections::BTreeSet<_>>();
    let exclude = policy
        .exclude
        .iter()
        .map(|key| key.trim().to_string())
        .filter(|key| !key.is_empty())
        .collect::<std::collections::BTreeSet<_>>();

    let inherited: Vec<(String, String)> = match policy.inherit {
        ShellInheritMode::None => Vec::new(),
        ShellInheritMode::All => host.into_iter().collect(),
        ShellInheritMode::Safe => host
            .into_iter()
            .filter(|(key, _)| safe_host_environment_key(key) || include_only.contains(key))
            .collect(),
    };

    let mut entries = std::collections::BTreeMap::new();
    for (key, value) in inherited {
        if key.starts_with("COWD_") || exclude.contains(&key) {
            continue;
        }
        if looks_like_secret(&key) && !include_only.contains(&key) {
            continue;
        }
        entries.insert(key, value);
    }
    for entry in policy.set {
        if entry.key.trim().is_empty() || entry.key.contains('\0') || entry.value.contains('\0') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("bash env.set entry `{}` is malformed", entry.key),
            ));
        }
        entries.insert(entry.key.clone(), entry.value.clone());
    }
    Ok(entries.into_iter().collect())
}

fn safe_host_environment_key(key: &str) -> bool {
    matches!(
        key,
        "LANG" | "LANGUAGE" | "TERM" | "COLORTERM" | "NO_COLOR" | "TZ"
    ) || key.starts_with("LC_")
        || matches!(key, "HTTP_PROXY" | "HTTPS_PROXY" | "ALL_PROXY" | "NO_PROXY")
}

fn looks_like_secret(key: &str) -> bool {
    let lowered = key.to_ascii_lowercase();
    [
        "token",
        "secret",
        "password",
        "passwd",
        "credential",
        "api_key",
        "apikey",
        "access_key",
        "accesskey",
        "private_key",
        "privatekey",
        "auth",
    ]
    .iter()
    .any(|marker| lowered.contains(marker))
}

fn build_output(
    input: &BashCommandInput,
    captured: CapturedOutput,
    interrupted: bool,
    sandbox_status: serde_json::Value,
    return_code_interpretation: Option<String>,
) -> BashCommandOutput {
    let stdout = String::from_utf8_lossy(&captured.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&captured.stderr).into_owned();
    let stdout_artifact = captured.stdout_artifact;
    let stderr_artifact = captured.stderr_artifact;
    let stdout_artifact_bytes = captured.stdout_artifact_bytes;
    let stderr_artifact_bytes = captured.stderr_artifact_bytes;
    let truncated = captured.stdout_truncated || captured.stderr_truncated;
    let persisted_output_path = stdout_artifact
        .or(stderr_artifact)
        .map(|path| path.to_string_lossy().into_owned());
    let persisted_output_size =
        (stdout_artifact_bytes + stderr_artifact_bytes).max(if persisted_output_path.is_some() {
            1
        } else {
            0
        });
    let no_output_expected = stdout.trim().is_empty() && stderr.trim().is_empty();
    BashCommandOutput {
        stdout,
        stderr,
        raw_output_path: None,
        interrupted,
        is_image: None,
        background_task_id: None,
        backgrounded_by_user: None,
        assistant_auto_backgrounded: None,
        dangerously_disable_sandbox: input.dangerously_disable_sandbox,
        return_code_interpretation,
        no_output_expected: Some(no_output_expected),
        structured_content: None,
        persisted_output_path,
        persisted_output_size: (persisted_output_size > 0).then_some(persisted_output_size),
        sandbox_status: Some(sandbox_status),
        return_truncated: truncated,
        progress: captured.samples,
    }
}

fn complete_sandbox_status(
    mut status: serde_json::Value,
    exit_code: Option<i32>,
    interrupted: bool,
) -> serde_json::Value {
    if let Some(object) = status.as_object_mut() {
        object.insert("state".to_string(), serde_json::json!("completed"));
        object.insert("ended_at_ms".to_string(), serde_json::json!(now_ms()));
        object.insert("exit_code".to_string(), serde_json::json!(exit_code));
        object.insert("interrupted".to_string(), serde_json::json!(interrupted));
    }
    status
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn environment_policy_defaults_mask_secrets_and_control_vars() {
        let result = apply_environment_policy(
            ShellEnvironmentPolicy::default(),
            vec![
                ("LANG".to_string(), "en_US.UTF-8".to_string()),
                ("API_TOKEN".to_string(), "secret".to_string()),
                ("COWD_CONTROL_ROOT".to_string(), "/control".to_string()),
                ("PATH".to_string(), "/usr/bin".to_string()),
            ],
        )
        .expect("policy");
        let result = result.into_iter().collect::<BTreeMap<_, _>>();
        assert!(result.get("LANG").is_some());
        assert!(result.get("API_TOKEN").is_none());
        assert!(result.get("COWD_CONTROL_ROOT").is_none());
        assert!(result.get("PATH").is_none());
    }

    #[test]
    fn model_escalation_fields_are_rejected_fail_closed() {
        let base = BashCommandInput {
            command: "printf ok".to_string(),
            cwd: None,
            timeout_ms: None,
            description: None,
            run_in_background: None,
            dangerously_disable_sandbox: None,
            isolate_network: None,
            workspace_access: None,
            allowed_mounts: None,
            env: None,
        };
        assert!(reject_model_escalation_fields(&base).is_ok());

        let mut mounts = base.clone();
        mounts.allowed_mounts = Some(vec!["/home/user/.ssh".to_string()]);
        assert!(reject_model_escalation_fields(&mounts)
            .expect_err("allowedMounts must be rejected")
            .contains("S-01"));

        let mut secrets = base.clone();
        secrets.env = Some(ShellEnvironmentPolicy {
            inherit: ShellInheritMode::Safe,
            include_only: vec!["CI_TOKEN".to_string()],
            exclude: Vec::new(),
            set: Vec::new(),
        });
        assert!(reject_model_escalation_fields(&secrets)
            .expect_err("include_only must be rejected")
            .contains("S-02"));

        let mut inherit_all = base.clone();
        inherit_all.env = Some(ShellEnvironmentPolicy {
            inherit: ShellInheritMode::All,
            include_only: Vec::new(),
            exclude: Vec::new(),
            set: Vec::new(),
        });
        assert!(reject_model_escalation_fields(&inherit_all)
            .expect_err("inherit=all must be rejected")
            .contains("S-02"));

        let mut background = base.clone();
        background.run_in_background = Some(true);
        assert!(reject_model_escalation_fields(&background)
            .expect_err("run_in_background must be rejected")
            .contains("S-03"));
    }

    #[test]
    fn workspace_access_is_independent_from_network_isolation() {
        let workspace = std::env::current_dir().expect("workspace");
        for (access, expected) in [
            (
                BashWorkspaceAccess::ReadOnly,
                SandboxWorkspaceAccess::ReadOnly,
            ),
            (
                BashWorkspaceAccess::ReadWrite,
                SandboxWorkspaceAccess::ReadWrite,
            ),
        ] {
            for isolate_network in [false, true] {
                let input = BashCommandInput {
                    command: "true".to_string(),
                    cwd: None,
                    timeout_ms: None,
                    description: None,
                    run_in_background: None,
                    dangerously_disable_sandbox: Some(false),
                    isolate_network: Some(isolate_network),
                    workspace_access: Some(access),
                    allowed_mounts: None,
                    env: None,
                };
                let spec = build_sandbox_spec(&input, &workspace, &workspace).expect("spec");
                assert_eq!(spec.workspace_access, expected);
                assert_eq!(spec.network_enabled, !isolate_network);
            }
        }

        let host = BashCommandInput {
            command: "true".to_string(),
            cwd: None,
            timeout_ms: None,
            description: None,
            run_in_background: None,
            dangerously_disable_sandbox: Some(true),
            isolate_network: Some(true),
            workspace_access: Some(BashWorkspaceAccess::ReadWrite),
            allowed_mounts: None,
            env: None,
        };
        let host_spec = build_sandbox_spec(&host, &workspace, &workspace).expect("host spec");
        assert_eq!(
            host_spec.workspace_access,
            SandboxWorkspaceAccess::ReadWrite
        );
        assert!(!host_spec.network_enabled);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn host_full_access_uses_real_host_backend_and_truthful_receipt() {
        let workspace = std::env::temp_dir().join(format!(
            "cowd-host-posture-test-{}-{}",
            std::process::id(),
            now_ms()
        ));
        std::fs::create_dir_all(&workspace).expect("host test workspace");
        let input = BashCommandInput {
            command: "test ! -e /proc/$$/fd/9 && printf host-write > host.txt".to_string(),
            cwd: None,
            timeout_ms: None,
            description: None,
            run_in_background: None,
            dangerously_disable_sandbox: Some(true),
            isolate_network: Some(false),
            workspace_access: Some(BashWorkspaceAccess::ReadWrite),
            allowed_mounts: None,
            env: None,
        };
        let prepared = prepare_command(&input, &workspace, &workspace, false)
            .expect("prepare real host command");
        assert_eq!(prepared.sandbox_status["backend"], "host");
        assert_eq!(
            prepared.sandbox_status["effective_posture"],
            "host-full-access"
        );
        assert_eq!(prepared.sandbox_status["protected_roots_denied"], false);
        assert_eq!(prepared.sandbox_status["inherited_fds_closed"], true);
        let program = prepared.command.get_program().to_os_string();
        let args = prepared
            .command
            .get_args()
            .map(std::ffi::OsStr::to_os_string)
            .collect::<Vec<_>>();
        let environment = prepared
            .command
            .get_envs()
            .filter_map(|(key, value)| {
                value.map(|value| (key.to_os_string(), value.to_os_string()))
            })
            .collect::<Vec<_>>();
        let current_dir = prepared
            .command
            .get_current_dir()
            .map(std::path::Path::to_path_buf);
        let mut inherited_fd_wrapper = Command::new("/bin/bash");
        inherited_fd_wrapper
            .arg("-c")
            .arg("exec 9</dev/null; exec \"$@\"")
            .arg("cowd-host-fd-fixture")
            .arg(program)
            .args(args)
            .env_clear()
            .envs(environment);
        if let Some(current_dir) = current_dir {
            inherited_fd_wrapper.current_dir(current_dir);
        }
        let output = inherited_fd_wrapper
            .output()
            .expect("execute host command with inherited descriptor");
        assert!(output.status.success(), "{output:?}");
        assert_eq!(
            std::fs::read_to_string(workspace.join("host.txt")).expect("host write"),
            "host-write"
        );
        std::fs::remove_dir_all(workspace).expect("remove host test workspace");
    }

    #[test]
    fn head_tail_capture_keeps_both_edges_and_marks_truncation() {
        let mut captured = CapturedStream::new();
        let payload = vec![b'x'; BASH_RETURN_HEAD_BYTES + 32];
        captured.push(&payload);
        let (rendered, _, _, truncated) = captured.finish();
        assert!(truncated);
        assert!(rendered
            .windows(b"[output truncated]".len())
            .any(|window| window == b"[output truncated]".as_slice()));
        assert!(rendered.ends_with(&payload[payload.len() - 16..]));
        assert!(rendered.starts_with(&payload[..16]));
    }

    #[test]
    fn async_capture_obeys_io_drain_contract() {
        // The async executor's bounded capture is exercised end-to-end in
        // `executor.rs` integration tests; here we only assert the progress
        // state machine.
        let mut state = BashProgressState::new();
        state.observe(StreamKind::Stdout, 100, None);
        assert!(!state.samples.is_empty());
        assert_eq!(state.stdout_bytes, 100);
        state.observe(StreamKind::Stderr, 50, None);
        assert_eq!(state.stderr_bytes, 50);
    }

    #[test]
    fn secret_detection_covers_common_variants() {
        for key in [
            "API_TOKEN",
            "GITHUB_TOKEN",
            "DB_PASSWORD",
            "PRIVATE_KEY",
            "AWS_ACCESS_KEY",
            "CLIENT_SECRET",
            "AUTH_HEADER",
        ] {
            assert!(looks_like_secret(key), "{key} should be masked");
        }
        for key in ["LANG", "TZ", "HOME", "PWD", "SHELL"] {
            assert!(!looks_like_secret(key), "{key} should not be masked");
        }
    }

    #[test]
    fn build_output_reports_sandbox_wiring_and_truncation() {
        let input = BashCommandInput {
            command: "printf x".to_string(),
            cwd: None,
            timeout_ms: None,
            description: None,
            run_in_background: None,
            dangerously_disable_sandbox: Some(true),
            isolate_network: Some(true),
            workspace_access: Some(BashWorkspaceAccess::ReadWrite),
            allowed_mounts: None,
            env: None,
        };
        let output = build_output(
            &input,
            CapturedOutput::from_bytes(b"", b"", None),
            false,
            BashCommandOutput::structured_sandbox_status(&input),
            None,
        );
        let status = output.sandbox_status.expect("sandbox status");
        assert_eq!(status["kernel_hardening_required"], false);
        assert_eq!(status["network_enabled"], false);
    }

    #[test]
    fn bash_timeout_wire_contract_rejects_ambiguous_legacy_field() {
        let input = serde_json::from_value::<BashCommandInput>(serde_json::json!({
            "command": "true",
            "timeout_ms": 2500
        }))
        .expect("explicit milliseconds");
        assert_eq!(input.timeout_ms, Some(2500));
        let legacy = serde_json::from_value::<BashCommandInput>(serde_json::json!({
            "command": "true",
            "timeout": 2500
        }));
        assert!(legacy.is_err(), "legacy timeout unit must fail closed");

        let root = env::current_dir().expect("timeout workspace");
        let invalid = BashCommandInput {
            command: "true".to_string(),
            cwd: None,
            timeout_ms: Some(MAX_TOOL_TIMEOUT_MS + 1),
            description: None,
            run_in_background: None,
            dangerously_disable_sandbox: Some(true),
            env: None,
            allowed_mounts: None,
            isolate_network: None,
            workspace_access: Some(BashWorkspaceAccess::ReadWrite),
        };
        let error = prepare_command(&invalid, &root, &root, false)
            .err()
            .expect("timeout above the wire maximum must fail closed");
        assert!(error.to_string().contains("between 1 and 300000"));
    }
}
