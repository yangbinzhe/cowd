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

use serde_json::{json, Value};

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
    /// Owning Session/execution id (J3), set by the runtime/gateway.
    owner: Option<String>,
    /// Cleanup evidence reason when the job was cancelled (J5).
    cleanup: Option<String>,
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

#[cfg(test)]
thread_local! {
    static LEDGER_DIR_OVERRIDE: std::cell::RefCell<Option<std::path::PathBuf>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn set_ledger_dir(dir: Option<std::path::PathBuf>) {
    LEDGER_DIR_OVERRIDE.with(|slot| *slot.borrow_mut() = dir);
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

    let started_at_ms = now_ms();
    let job = Arc::new(Job {
        job_id: job_id.clone(),
        pid,
        command: command.clone(),
        cwd,
        started_at_ms,
        stdout,
        stderr,
        state: Arc::clone(&state),
        notified: AtomicBool::new(false),
    });
    let ledger = ledger_dir();
    if let Some(dir) = ledger.as_deref() {
        write_ledger_in(dir, &job_id, pid, started_at_ms, &command, None, None, None);
    }

    let ledger_job_id = job_id.clone();
    let ledger_command = command.clone();
    std::thread::spawn(move || {
        let status = child.wait();
        let exit_code = status.ok().and_then(|status| status.code());
        let finished_at_ms = now_ms();
        let (owner, cleanup) = {
            let mut guard = lock(&state);
            guard.finished = true;
            guard.exit_code = exit_code;
            guard.finished_at_ms = Some(finished_at_ms);
            (guard.owner.clone(), guard.cleanup.clone())
        };
        if let Some(dir) = ledger.as_deref() {
            write_ledger_in(
                dir,
                &ledger_job_id,
                pid,
                started_at_ms,
                &ledger_command,
                Some((exit_code, finished_at_ms)),
                owner.as_deref(),
                cleanup.as_deref(),
            );
        }
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

/// J3: attach the owning Session/execution to a live job. Called by the
/// runtime/gateway when it observes a `backgroundTaskId` in a tool result.
pub fn register_owner(workspace_root: &Path, job_id: &str, owner_id: &str) -> bool {
    match find(workspace_root, job_id) {
        Some(job) => {
            lock(&job.state).owner = Some(owner_id.to_string());
            true
        }
        None => false,
    }
}

/// J3: kill every live job owned by `owner_id` (across workspaces), recording
/// `reason` as cleanup evidence. Returns the killed job ids.
pub fn kill_by_owner(owner_id: &str, reason: &str) -> Vec<String> {
    let jobs: Vec<Arc<Job>> = {
        let map = lock(registry());
        map.values()
            .filter(|job| {
                let guard = lock(&job.state);
                guard.owner.as_deref() == Some(owner_id) && !guard.finished
            })
            .cloned()
            .collect()
    };
    let mut killed = Vec::new();
    for job in jobs {
        lock(&job.state).cleanup = Some(reason.to_string());
        if kill_process_group(job.pid) {
            killed.push(job.job_id.clone());
        }
    }
    killed
}

/// Durable pid ledger directory (survives process restart).
fn ledger_dir() -> Option<std::path::PathBuf> {
    #[cfg(test)]
    if let Some(dir) = LEDGER_DIR_OVERRIDE.with(|slot| slot.borrow().clone()) {
        std::fs::create_dir_all(&dir).ok()?;
        return Some(dir);
    }
    let base = std::env::var_os("COWD_BASH_ARTIFACT_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var_os("HOME")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(std::env::temp_dir)
                .join(".cowd")
                .join("storage")
                .join("bash-artifacts")
        })
        .join("bg-jobs");
    std::fs::create_dir_all(&base).ok()?;
    Some(base)
}

fn write_ledger_in(
    dir: &Path,
    job_id: &str,
    pid: u32,
    started_at_ms: u64,
    command: &str,
    terminal: Option<(Option<i32>, u64)>,
    owner: Option<&str>,
    cleanup: Option<&str>,
) {
    let value = json!({
        "jobId": job_id,
        "pid": pid,
        // process_group(0) makes the child its own group leader, so pgid == pid.
        "pgid": pid,
        "startedAtMs": started_at_ms,
        "command": command,
        "finishedAtMs": terminal.map(|(_, at)| at),
        "exitCode": terminal.and_then(|(code, _)| code),
        "owner": owner,
        "cleanup": cleanup,
    });
    let _ = std::fs::write(dir.join(format!("{job_id}.json")), value.to_string());
}

/// Outcome of a startup reconciliation pass.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    pub examined: usize,
    pub reaped: usize,
    pub marked_finished: usize,
}

fn process_group_alive(pgid: u32) -> bool {
    #[cfg(unix)]
    {
        std::process::Command::new("kill")
            .args(["-0", "--", &format!("-{pgid}")])
            .status()
            .is_ok_and(|status| status.success())
    }
    #[cfg(not(unix))]
    {
        let _ = pgid;
        false
    }
}

/// Startup reconciliation (J2). Call once, before this process spawns any job:
/// every ledger entry is therefore owned by a previous process, so a still-live
/// process group is an orphan and is reaped; a dead one is marked terminal.
/// Idempotent.
pub fn reconcile_background_jobs() -> ReconcileReport {
    let mut report = ReconcileReport::default();
    let Some(dir) = ledger_dir() else {
        return report;
    };
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return report;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension() != Some(std::ffi::OsStr::new("json")) {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Ok(mut value) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        report.examined += 1;
        if value
            .get("finishedAtMs")
            .is_some_and(|finished| !finished.is_null())
        {
            continue;
        }
        let Some(pgid) = value.get("pgid").and_then(Value::as_u64).map(|v| v as u32) else {
            continue;
        };
        if process_group_alive(pgid) {
            kill_process_group(pgid);
            report.reaped += 1;
        } else {
            report.marked_finished += 1;
        }
        if let Some(object) = value.as_object_mut() {
            object.insert("finishedAtMs".to_string(), json!(now_ms()));
            object.insert("reconciled".to_string(), json!(true));
        }
        let _ = std::fs::write(&path, value.to_string());
    }
    report
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

    #[test]
    fn ledger_records_terminal_state() {
        let dir = std::env::temp_dir().join(format!("bg-ledger-{}", now_ms()));
        let jobs = dir.join("bg-jobs");
        std::fs::create_dir_all(&jobs).unwrap();
        set_ledger_dir(Some(jobs.clone()));
        let root = std::env::temp_dir();
        let command = "echo ledger-check";
        let job_id =
            start_background_job(spawn(command), command.to_string(), "/tmp".into(), &root);
        let _ = wait_until_finished(&root, &job_id);
        let ledger = jobs.join(format!("{job_id}.json"));
        let mut finished = false;
        for _ in 0..200 {
            if let Ok(text) = std::fs::read_to_string(&ledger) {
                if serde_json::from_str::<Value>(&text)
                    .map(|value| value["finishedAtMs"].is_number())
                    .unwrap_or(false)
                {
                    finished = true;
                    break;
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(finished, "ledger must record the terminal state");
        set_ledger_dir(None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reconcile_marks_dead_and_leaves_finished() {
        let dir = std::env::temp_dir().join(format!("bg-reconcile-{}", now_ms()));
        let jobs = dir.join("bg-jobs");
        std::fs::create_dir_all(&jobs).unwrap();
        set_ledger_dir(Some(jobs.clone()));
        std::fs::write(
            jobs.join("bg-dead.json"),
            json!({"jobId":"bg-dead","pid":4000000,"pgid":4000000,"startedAtMs":1,"command":"x"})
                .to_string(),
        )
        .unwrap();
        std::fs::write(
            jobs.join("bg-done.json"),
            json!({"jobId":"bg-done","pid":1,"pgid":1,"startedAtMs":1,"command":"x","finishedAtMs":2})
                .to_string(),
        )
        .unwrap();

        let report = reconcile_background_jobs();
        assert_eq!(report.examined, 2);
        assert_eq!(report.marked_finished, 1);
        assert_eq!(report.reaped, 0);
        let dead: Value =
            serde_json::from_str(&std::fs::read_to_string(jobs.join("bg-dead.json")).unwrap())
                .unwrap();
        assert_eq!(dead["reconciled"], json!(true));
        let done: Value =
            serde_json::from_str(&std::fs::read_to_string(jobs.join("bg-done.json")).unwrap())
                .unwrap();
        assert_eq!(done["finishedAtMs"], json!(2));

        set_ledger_dir(None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn owner_registration_fences_jobs_by_session() {
        let dir = std::env::temp_dir().join(format!("bg-owner-{}", now_ms()));
        let jobs = dir.join("bg-jobs");
        std::fs::create_dir_all(&jobs).unwrap();
        set_ledger_dir(Some(jobs.clone()));
        let root = std::env::temp_dir();
        let command = "sleep 30";
        let job_id =
            start_background_job(spawn(command), command.to_string(), "/tmp".into(), &root);

        assert!(register_owner(&root, &job_id, "session-A"));
        let killed = kill_by_owner("session-A", "session_closed");
        assert_eq!(killed, vec![job_id.clone()]);
        let status = wait_until_finished(&root, &job_id);
        assert_eq!(status["running"], json!(false));
        // A different owner fences nothing.
        assert!(kill_by_owner("session-B", "session_closed").is_empty());

        set_ledger_dir(None);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
