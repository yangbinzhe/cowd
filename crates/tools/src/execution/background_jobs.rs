//! Process-local background-job registry for `bash run_in_background` (S-03/A2).
//!
//! Phase 1 (this module): jobs live in this process only, keyed by workspace
//! root. stdout/stderr are drained by capture threads (head+tail+persist via
//! `CapturedStream`, reusing the existing bounded output budget); a monitor
//! thread records the terminal state (exit code + finished-at). A completed job
//! is surfaced to the next model turn by the runtime via `take_completions`.
//!
//! Not yet implemented (Phase 2, honest boundary): cross-process/restart
//! durability, Session-deletion fence cleanup, and restart orphan-PID reaping.
//! Jobs therefore do not survive a Cowd process restart; only the persisted
//! output artifacts remain.

use std::collections::HashMap;
use std::io::Read;
use std::path::Path;
use std::process::Child;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::json;

use crate::bash::CapturedStream;

/// Bound on the live tail returned by `background_task_status`.
const MAX_TAIL_BYTES: usize = 64 * 1024;
/// Finished jobs older than this are pruned from the registry.
const JOB_RETENTION_MS: u64 = 24 * 60 * 60 * 1000;

#[derive(Default)]
struct JobState {
    finished: bool,
    exit_code: Option<i32>,
    finished_at_ms: Option<u64>,
}

struct Job {
    job_id: String,
    pid: u32,
    command: String,
    cwd: String,
    started_at_ms: u64,
    stdout: Arc<Mutex<CapturedStream>>,
    stderr: Arc<Mutex<CapturedStream>>,
    state: Arc<Mutex<JobState>>,
    notified: AtomicBool,
}

fn registry() -> &'static Mutex<HashMap<(String, String), Arc<Job>>> {
    static JOBS: OnceLock<Mutex<HashMap<(String, String), Arc<Job>>>> = OnceLock::new();
    JOBS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn workspace_key(root: &Path) -> String {
    root.canonicalize()
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|_| root.to_string_lossy().into_owned())
}

/// Register a spawned background child and start draining/monitoring it.
pub(crate) fn start_background_job(
    child: Child,
    command: String,
    cwd: String,
    workspace_root: &Path,
) -> String {
    let mut child = child;
    let pid = child.id();
    let job_id = format!("bg-{pid}-{}", now_ms());
    let stdout = Arc::new(Mutex::new(CapturedStream::new()));
    let stderr = Arc::new(Mutex::new(CapturedStream::new()));
    let state = Arc::new(Mutex::new(JobState::default()));

    if let Some(stream) = child.stdout.take() {
        let capture = Arc::clone(&stdout);
        std::thread::spawn(move || drain(stream, capture));
    }
    if let Some(stream) = child.stderr.take() {
        let capture = Arc::clone(&stderr);
        std::thread::spawn(move || drain(stream, capture));
    }

    let job = Arc::new(Job {
        job_id: job_id.clone(),
        pid,
        command,
        cwd,
        started_at_ms: now_ms(),
        stdout,
        stderr,
        state: Arc::clone(&state),
        notified: AtomicBool::new(false),
    });

    std::thread::spawn(move || {
        let status = child.wait();
        let mut guard = lock(&state);
        guard.finished = true;
        guard.exit_code = status.ok().and_then(|status| status.code());
        guard.finished_at_ms = Some(now_ms());
    });

    lock(registry()).insert((workspace_key(workspace_root), job_id.clone()), job);
    job_id
}

fn drain(mut stream: impl Read, capture: Arc<Mutex<CapturedStream>>) {
    let mut buffer = [0u8; 16 * 1024];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => lock(&capture).push(&buffer[..read]),
            Err(_) => break,
        }
    }
}

fn find(workspace_root: &Path, job_id: &str) -> Option<Arc<Job>> {
    lock(registry())
        .get(&(workspace_key(workspace_root), job_id.to_string()))
        .cloned()
}

/// Read-only status / incremental tail for one job.
pub(crate) fn job_status(
    workspace_root: &Path,
    job_id: &str,
    tail: Option<usize>,
) -> Result<String, String> {
    let job = find(workspace_root, job_id)
        .ok_or_else(|| format!("background job `{job_id}` not found"))?;
    let state = lock(&job.state);
    let stdout = lock(&job.stdout);
    let stderr = lock(&job.stderr);
    let tail_limit = tail.unwrap_or(0).min(MAX_TAIL_BYTES);
    let (stdout_text, stderr_text) = if tail_limit > 0 {
        (
            stdout.tail_string(tail_limit),
            stderr.tail_string(tail_limit),
        )
    } else {
        (stdout.preview(), stderr.preview())
    };
    let value = json!({
        "jobId": job.job_id,
        "pid": job.pid,
        "command": job.command,
        "cwd": job.cwd,
        "running": !state.finished,
        "exitCode": state.exit_code,
        "startedAtMs": job.started_at_ms,
        "finishedAtMs": state.finished_at_ms,
        "elapsedMs": now_ms().saturating_sub(job.started_at_ms),
        "stdoutBytes": stdout.total_bytes(),
        "stderrBytes": stderr.total_bytes(),
        "truncated": stdout.is_truncated() || stderr.is_truncated(),
        "persistedOutputPath": stdout
            .artifact_path()
            .or_else(|| stderr.artifact_path())
            .map(|path| path.to_string_lossy().into_owned()),
        "stdout": stdout_text,
        "stderr": stderr_text,
    });
    serde_json::to_string_pretty(&value).map_err(|error| error.to_string())
}

/// Kill a job's process group. Terminal state is recorded by the monitor thread.
pub(crate) fn kill_job(workspace_root: &Path, job_id: &str) -> Result<String, String> {
    let job = find(workspace_root, job_id)
        .ok_or_else(|| format!("background job `{job_id}` not found"))?;
    let killed = kill_process_group(job.pid);
    serde_json::to_string_pretty(&json!({
        "jobId": job.job_id,
        "pid": job.pid,
        "killed": killed,
    }))
    .map_err(|error| error.to_string())
}

fn kill_process_group(pid: u32) -> bool {
    #[cfg(unix)]
    {
        let group = format!("-{pid}");
        let group_killed = std::process::Command::new("kill")
            .args(["-KILL", "--", &group])
            .status()
            .is_ok_and(|status| status.success());
        return group_killed
            || std::process::Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .status()
                .is_ok_and(|status| status.success());
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

/// One completed job the runtime should surface to the next model turn.
pub struct BackgroundCompletion {
    pub job_id: String,
    pub summary: String,
}

/// Drain finished-and-unnotified jobs for a workspace, marking them notified.
pub fn take_completions(workspace_root: &Path) -> Vec<BackgroundCompletion> {
    let key = workspace_key(workspace_root);
    let mut completions = Vec::new();
    let mut map = lock(registry());
    map.retain(|(job_workspace, _), job| {
        if job_workspace != &key {
            return true;
        }
        let state = lock(&job.state);
        let expired = state.finished
            && state
                .finished_at_ms
                .is_some_and(|at| now_ms().saturating_sub(at) > JOB_RETENTION_MS);
        !(expired && job.notified.load(Ordering::SeqCst))
    });
    for ((job_workspace, _), job) in map.iter() {
        if job_workspace != &key {
            continue;
        }
        let (finished, exit_code) = {
            let state = lock(&job.state);
            (state.finished, state.exit_code)
        };
        if !finished || job.notified.swap(true, Ordering::SeqCst) {
            continue;
        }
        let elapsed_secs = now_ms().saturating_sub(job.started_at_ms) / 1000;
        let tail = lock(&job.stdout).tail_string(2048);
        let outcome = exit_code
            .map(|code| format!("exit {code}"))
            .unwrap_or_else(|| "terminated by signal".to_string());
        let recent = if tail.trim().is_empty() {
            "(no output)".to_string()
        } else {
            tail.trim().to_string()
        };
        completions.push(BackgroundCompletion {
            job_id: job.job_id.clone(),
            summary: format!(
                "Background job {} finished ({outcome}, {elapsed_secs}s): `{}`\nRecent output:\n{recent}",
                job.job_id, job.command
            ),
        });
    }
    completions
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::process::{Command, Stdio};

    fn spawn(command: &str) -> Child {
        let mut child = Command::new("/bin/bash");
        child
            .arg("-c")
            .arg(command)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        child.spawn().expect("spawn")
    }

    fn wait_until_finished(root: &Path, job_id: &str) -> Value {
        for _ in 0..200 {
            let status: Value =
                serde_json::from_str(&job_status(root, job_id, None).expect("status"))
                    .expect("json");
            if status["running"] == json!(false) {
                return status;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        panic!("job did not finish");
    }

    #[test]
    fn captures_output_and_terminal_state() {
        let root = std::env::temp_dir();
        let command = "echo hello-bg; echo err-bg 1>&2";
        let job_id =
            start_background_job(spawn(command), command.to_string(), "/tmp".into(), &root);
        let status = wait_until_finished(&root, &job_id);
        assert_eq!(status["exitCode"], json!(0));
        assert!(status["stdout"].as_str().unwrap().contains("hello-bg"));
        assert!(status["stderr"].as_str().unwrap().contains("err-bg"));

        let completions = take_completions(&root);
        assert!(completions.iter().any(|c| c.job_id == job_id));
        // Second drain is empty: notified exactly once.
        assert!(!take_completions(&root).iter().any(|c| c.job_id == job_id));
    }

    #[test]
    fn kill_stops_a_long_running_job() {
        let root = std::env::temp_dir();
        let command = "sleep 30";
        let job_id =
            start_background_job(spawn(command), command.to_string(), "/tmp".into(), &root);
        let killed: Value = serde_json::from_str(&kill_job(&root, &job_id).expect("kill")).unwrap();
        assert_eq!(killed["killed"], json!(true));
        let status = wait_until_finished(&root, &job_id);
        assert_eq!(status["running"], json!(false));
    }

    #[test]
    fn unknown_job_is_a_clear_error() {
        let root = std::env::temp_dir();
        assert!(job_status(&root, "bg-nope", None).is_err());
        assert!(kill_job(&root, "bg-nope").is_err());
    }
}
