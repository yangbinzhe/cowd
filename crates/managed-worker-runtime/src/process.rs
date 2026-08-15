use std::{
    collections::BTreeMap,
    os::unix::{
        fs::{FileTypeExt, MetadataExt, PermissionsExt},
        process::CommandExt,
    },
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{
    process::{Child, Command},
    sync::Mutex,
    task::JoinHandle,
    time::Instant,
};

use crate::{
    log_buffer::BoundedLogBuffer, CancellationToken, CredentialLease, CredentialSecret,
    GenerationFence, LogSnapshot, ManagedWorkerError, ManagedWorkerResult, WorkerRuntimeDir,
};

const POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Debug, Clone)]
pub struct ManagedWorkerSpec {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub runtime_dir: PathBuf,
    pub generation: String,
    pub startup_timeout: Duration,
    pub graceful_shutdown_timeout: Duration,
    pub log_capacity_bytes: usize,
    pub require_socket: bool,
    pub socket_env: Option<String>,
    pub credential_env: Option<String>,
    pub generation_env: Option<String>,
}

impl ManagedWorkerSpec {
    #[must_use]
    pub fn new(
        program: impl Into<PathBuf>,
        runtime_dir: impl Into<PathBuf>,
        generation: impl Into<String>,
    ) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            env: BTreeMap::new(),
            runtime_dir: runtime_dir.into(),
            generation: generation.into(),
            startup_timeout: Duration::from_secs(10),
            graceful_shutdown_timeout: Duration::from_secs(5),
            log_capacity_bytes: 256 * 1024,
            require_socket: true,
            socket_env: None,
            credential_env: None,
            generation_env: None,
        }
    }

    #[must_use]
    pub fn args(mut self, args: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.args = args.into_iter().map(Into::into).collect();
        self
    }

    #[must_use]
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }

    fn validate(&self) -> ManagedWorkerResult<()> {
        if self.program.as_os_str().is_empty() {
            return Err(ManagedWorkerError::InvalidSpec(
                "program path is empty".to_string(),
            ));
        }
        if self.startup_timeout.is_zero() || self.graceful_shutdown_timeout.is_zero() {
            return Err(ManagedWorkerError::InvalidSpec(
                "startup and shutdown timeouts must be positive".to_string(),
            ));
        }
        if self.log_capacity_bytes == 0 {
            return Err(ManagedWorkerError::InvalidSpec(
                "log capacity must be positive".to_string(),
            ));
        }
        for name in [
            self.socket_env.as_deref(),
            self.credential_env.as_deref(),
            self.generation_env.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if name.trim().is_empty() || name.contains('=') || name.contains('\0') {
                return Err(ManagedWorkerError::InvalidSpec(format!(
                    "invalid injected environment name `{name}`"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerExit {
    pub code: Option<i32>,
    pub success: bool,
}

impl From<ExitStatus> for WorkerExit {
    fn from(status: ExitStatus) -> Self {
        Self {
            code: status.code(),
            success: status.success(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ManagedWorkerHandle {
    inner: Arc<ManagedWorkerInner>,
}

#[derive(Debug)]
struct ManagedWorkerInner {
    runtime: WorkerRuntimeDir,
    generation: GenerationFence,
    credential: CredentialLease,
    bootstrap_secret: Mutex<Option<CredentialSecret>>,
    child: Mutex<Option<Child>>,
    exit: Mutex<Option<WorkerExit>>,
    pid: u32,
    stdout: BoundedLogBuffer,
    stderr: BoundedLogBuffer,
    drain_tasks: Mutex<Vec<JoinHandle<()>>>,
    graceful_shutdown_timeout: Duration,
    closed: AtomicBool,
}

impl ManagedWorkerHandle {
    pub async fn spawn(spec: ManagedWorkerSpec) -> ManagedWorkerResult<Self> {
        spec.validate()?;
        let generation = GenerationFence::new(spec.generation.clone())?;
        let runtime = WorkerRuntimeDir::create(&spec.runtime_dir)?;
        runtime.cleanup_ephemeral()?;
        let (credential, bootstrap_secret) = CredentialLease::create(&runtime)?;

        let mut command = Command::new(&spec.program);
        command
            .args(&spec.args)
            .env_clear()
            .envs(&spec.env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command.as_std_mut().process_group(0);
        if let Some(name) = &spec.socket_env {
            command.env(name, runtime.socket_path());
        }
        if let Some(name) = &spec.credential_env {
            command.env(name, credential.path());
        }
        if let Some(name) = &spec.generation_env {
            command.env(name, generation.as_str());
        }
        let mut child = command
            .spawn()
            .map_err(|error| ManagedWorkerError::io(&spec.program, error))?;
        let pid = child.id().ok_or_else(|| {
            ManagedWorkerError::InvalidSpec("spawned worker has no process id".to_string())
        })?;
        let stdout = BoundedLogBuffer::new(spec.log_capacity_bytes);
        let stderr = BoundedLogBuffer::new(spec.log_capacity_bytes);
        let mut drain_tasks = Vec::with_capacity(2);
        if let Some(pipe) = child.stdout.take() {
            let buffer = stdout.clone();
            drain_tasks.push(tokio::spawn(async move { buffer.drain(pipe).await }));
        }
        if let Some(pipe) = child.stderr.take() {
            let buffer = stderr.clone();
            drain_tasks.push(tokio::spawn(async move { buffer.drain(pipe).await }));
        }
        let handle = Self {
            inner: Arc::new(ManagedWorkerInner {
                runtime,
                generation,
                credential,
                bootstrap_secret: Mutex::new(Some(bootstrap_secret)),
                child: Mutex::new(Some(child)),
                exit: Mutex::new(None),
                pid,
                stdout,
                stderr,
                drain_tasks: Mutex::new(drain_tasks),
                graceful_shutdown_timeout: spec.graceful_shutdown_timeout,
                closed: AtomicBool::new(false),
            }),
        };
        if spec.require_socket {
            let cancellation = CancellationToken::default();
            if let Err(error) = handle
                .wait_for_socket(spec.startup_timeout, &cancellation)
                .await
            {
                let _ = handle.shutdown().await;
                return Err(error);
            }
        }
        Ok(handle)
    }

    #[must_use]
    pub fn pid(&self) -> u32 {
        self.inner.pid
    }

    #[must_use]
    pub fn generation(&self) -> &GenerationFence {
        &self.inner.generation
    }

    #[must_use]
    pub fn socket_path(&self) -> PathBuf {
        self.inner.runtime.socket_path()
    }

    #[must_use]
    pub fn credential_path(&self) -> &Path {
        self.inner.credential.path()
    }

    /// Transfer the in-memory bootstrap copy exactly once to the domain handshake owner.
    pub async fn take_bootstrap_secret(&self) -> ManagedWorkerResult<CredentialSecret> {
        self.inner
            .bootstrap_secret
            .lock()
            .await
            .take()
            .ok_or(ManagedWorkerError::CredentialConsumed)
    }

    pub async fn wait_for_socket(
        &self,
        timeout: Duration,
        cancellation: &CancellationToken,
    ) -> ManagedWorkerResult<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if cancellation.is_cancelled() {
                return Err(ManagedWorkerError::Cancelled);
            }
            if let Some(exit) = self.try_wait().await? {
                return Err(ManagedWorkerError::ExitedBeforeReady(format!(
                    "code={:?}",
                    exit.code
                )));
            }
            match std::fs::symlink_metadata(self.socket_path()) {
                Ok(metadata) if metadata.file_type().is_socket() => {
                    if metadata.uid() != self.inner.runtime.owner_uid() {
                        return Err(ManagedWorkerError::InvalidSpec(format!(
                            "worker socket owner uid {} differs from runtime uid {}",
                            metadata.uid(),
                            self.inner.runtime.owner_uid()
                        )));
                    }
                    std::fs::set_permissions(
                        self.socket_path(),
                        std::fs::Permissions::from_mode(0o600),
                    )
                    .map_err(|error| ManagedWorkerError::io(self.socket_path(), error))?;
                    return Ok(());
                }
                Ok(_) => {
                    return Err(ManagedWorkerError::InvalidSpec(format!(
                        "worker socket path is not a Unix socket: {}",
                        self.socket_path().display()
                    )))
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(ManagedWorkerError::io(self.socket_path(), error)),
            }
            if Instant::now() >= deadline {
                return Err(ManagedWorkerError::DeadlineExceeded(timeout));
            }
            tokio::select! {
                () = cancellation.cancelled() => return Err(ManagedWorkerError::Cancelled),
                () = tokio::time::sleep(POLL_INTERVAL) => {}
            }
        }
    }

    pub async fn try_wait(&self) -> ManagedWorkerResult<Option<WorkerExit>> {
        if let Some(exit) = self.inner.exit.lock().await.clone() {
            return Ok(Some(exit));
        }
        let mut child = self.inner.child.lock().await;
        let Some(process) = child.as_mut() else {
            return Ok(None);
        };
        match process
            .try_wait()
            .map_err(|error| ManagedWorkerError::io("worker-process", error))?
        {
            Some(status) => {
                child.take();
                let exit = WorkerExit::from(status);
                *self.inner.exit.lock().await = Some(exit.clone());
                Ok(Some(exit))
            }
            None => Ok(None),
        }
    }

    pub async fn wait_for_exit(
        &self,
        timeout: Duration,
        cancellation: &CancellationToken,
    ) -> ManagedWorkerResult<WorkerExit> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(exit) = self.try_wait().await? {
                self.finish_cleanup().await;
                return Ok(exit);
            }
            if Instant::now() >= deadline {
                return Err(ManagedWorkerError::DeadlineExceeded(timeout));
            }
            tokio::select! {
                () = cancellation.cancelled() => return Err(ManagedWorkerError::Cancelled),
                () = tokio::time::sleep(POLL_INTERVAL) => {}
            }
        }
    }

    pub async fn stdout(&self) -> LogSnapshot {
        self.inner.stdout.snapshot().await
    }

    pub async fn stderr(&self) -> LogSnapshot {
        self.inner.stderr.snapshot().await
    }

    pub async fn shutdown(&self) -> ManagedWorkerResult<WorkerExit> {
        if let Some(exit) = self.try_wait().await? {
            self.finish_cleanup().await;
            return Ok(exit);
        }
        if let Err(signal_error) = signal_process_group(self.pid(), "TERM").await {
            if let Some(exit) = self.try_wait().await? {
                self.finish_cleanup().await;
                return Ok(exit);
            }
            return Err(signal_error);
        }
        let wait = self
            .wait_for_exit(
                self.inner.graceful_shutdown_timeout,
                &CancellationToken::default(),
            )
            .await;
        match wait {
            Ok(exit) => Ok(exit),
            Err(ManagedWorkerError::DeadlineExceeded(_)) => {
                if let Err(signal_error) = signal_process_group(self.pid(), "KILL").await {
                    if let Some(exit) = self.try_wait().await? {
                        self.finish_cleanup().await;
                        return Ok(exit);
                    }
                    return Err(signal_error);
                }
                self.wait_for_exit(Duration::from_secs(1), &CancellationToken::default())
                    .await
            }
            Err(error) => Err(error),
        }
    }

    async fn finish_cleanup(&self) {
        if self.inner.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.inner.bootstrap_secret.lock().await.take();
        let tasks = std::mem::take(&mut *self.inner.drain_tasks.lock().await);
        for task in tasks {
            let _ = tokio::time::timeout(Duration::from_secs(1), task).await;
        }
        if let Err(error) = self.inner.runtime.cleanup_ephemeral() {
            tracing::warn!(%error, "managed worker runtime cleanup failed");
        }
    }
}

impl Drop for ManagedWorkerInner {
    fn drop(&mut self) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let _ = std::process::Command::new("kill")
            .args(["-KILL", "--", &format!("-{}", self.pid)])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if let Ok(mut child) = self.child.try_lock() {
            if let Some(child) = child.as_mut() {
                let _ = child.start_kill();
            }
        }
        let _ = self.runtime.cleanup_ephemeral();
    }
}

async fn signal_process_group(pid: u32, signal: &str) -> ManagedWorkerResult<()> {
    let status = Command::new("kill")
        .args([format!("-{signal}"), "--".to_string(), format!("-{pid}")])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .status()
        .await
        .map_err(|error| ManagedWorkerError::Signal(error.to_string()))?;
    if status.success() {
        Ok(())
    } else {
        Err(ManagedWorkerError::Signal(format!(
            "kill -{signal} process-group {pid} exited {status}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, os::unix::fs::PermissionsExt};

    fn temp_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "managed-worker-process-{label}-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ))
    }

    fn shell_spec(label: &str, script: &str) -> ManagedWorkerSpec {
        let mut spec = ManagedWorkerSpec::new("/bin/sh", temp_path(label), "generation-1")
            .args(["-c", script]);
        spec.require_socket = false;
        spec.startup_timeout = Duration::from_millis(100);
        spec.graceful_shutdown_timeout = Duration::from_millis(150);
        spec.log_capacity_bytes = 1024;
        spec
    }

    #[tokio::test]
    async fn spawn_crash_and_log_drain_are_observable_and_bounded() {
        let handle = ManagedWorkerHandle::spawn(shell_spec(
            "crash-log",
            "i=0; while [ $i -lt 400 ]; do printf x; printf y >&2; i=$((i+1)); done; exit 7",
        ))
        .await
        .expect("spawn");
        let runtime = handle.inner.runtime.root().to_path_buf();
        let exit = handle
            .wait_for_exit(Duration::from_secs(2), &CancellationToken::default())
            .await
            .expect("exit");
        assert_eq!(exit.code, Some(7));
        assert_eq!(handle.stdout().await.bytes.len(), 400);
        assert_eq!(handle.stderr().await.bytes.len(), 400);
        assert_eq!(handle.try_wait().await.expect("cached exit"), Some(exit));
        assert!(!handle.socket_path().exists());
        assert!(!handle.credential_path().exists());
        fs::remove_dir_all(runtime).expect("cleanup");
    }

    #[tokio::test]
    async fn logs_keep_a_bounded_tail_without_blocking_child() {
        let mut spec = shell_spec(
            "bounded-log",
            "i=0; while [ $i -lt 8192 ]; do printf z; i=$((i+1)); done",
        );
        spec.log_capacity_bytes = 256;
        let handle = ManagedWorkerHandle::spawn(spec).await.expect("spawn");
        let runtime = handle.inner.runtime.root().to_path_buf();
        handle
            .wait_for_exit(Duration::from_secs(2), &CancellationToken::default())
            .await
            .expect("exit");
        let log = handle.stdout().await;
        assert_eq!(log.bytes.len(), 256);
        assert_eq!(log.dropped_bytes, 8192 - 256);
        fs::remove_dir_all(runtime).expect("cleanup");
    }

    #[tokio::test]
    async fn cancelled_wait_does_not_reap_or_kill_shared_worker() {
        let handle = ManagedWorkerHandle::spawn(shell_spec("cancel", "sleep 60"))
            .await
            .expect("spawn");
        let runtime = handle.inner.runtime.root().to_path_buf();
        let token = CancellationToken::default();
        token.cancel();
        assert!(matches!(
            handle.wait_for_exit(Duration::from_secs(1), &token).await,
            Err(ManagedWorkerError::Cancelled)
        ));
        assert!(handle.try_wait().await.expect("poll").is_none());
        handle.shutdown().await.expect("shutdown");
        fs::remove_dir_all(runtime).expect("cleanup");
    }

    #[tokio::test]
    async fn startup_timeout_kills_process_group_and_cleans_runtime_entries() {
        let root = temp_path("startup-timeout");
        let mut spec =
            ManagedWorkerSpec::new("/bin/sh", &root, "generation-1").args(["-c", "sleep 60"]);
        spec.startup_timeout = Duration::from_millis(50);
        spec.graceful_shutdown_timeout = Duration::from_millis(100);
        assert!(matches!(
            ManagedWorkerHandle::spawn(spec).await,
            Err(ManagedWorkerError::DeadlineExceeded(_))
        ));
        assert!(!root.join("worker.sock").exists());
        assert!(!root.join("credential").exists());
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[tokio::test]
    async fn ready_socket_is_owner_only_and_removed_on_shutdown() {
        let root = temp_path("ready-socket");
        let script = r#"
import os
import socket
import time

listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
listener.bind(os.environ["WORKER_SOCKET"])
listener.listen(8)
time.sleep(60)
"#;
        let mut spec =
            ManagedWorkerSpec::new("/usr/bin/python3", &root, "generation-1").args(["-c", script]);
        spec.socket_env = Some("WORKER_SOCKET".to_string());
        spec.startup_timeout = Duration::from_secs(2);
        spec.graceful_shutdown_timeout = Duration::from_millis(150);
        let handle = ManagedWorkerHandle::spawn(spec).await.expect("spawn");
        let metadata = fs::symlink_metadata(handle.socket_path()).expect("socket metadata");
        assert!(metadata.file_type().is_socket());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        handle.shutdown().await.expect("shutdown");
        assert!(!handle.socket_path().exists());
        assert!(!handle.credential_path().exists());
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[tokio::test]
    async fn shutdown_kills_descendant_process_group_and_removes_socket() {
        let mut spec = shell_spec("process-group", "unused");
        spec.env.insert(
            "PATH".to_string(),
            std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".to_string()),
        );
        let stable_root = spec.runtime_dir.clone();
        let pid_file = stable_root.join("descendant.pid");
        spec.args = vec![
            "-c".to_string(),
            format!(
                "sleep 60 & child=$!; echo $child > {}; wait",
                pid_file.display()
            ),
        ];
        let handle = ManagedWorkerHandle::spawn(spec)
            .await
            .expect("spawn stable");
        for _ in 0..100 {
            if pid_file.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let descendant = fs::read_to_string(&pid_file).expect("descendant pid");
        fs::write(handle.socket_path(), b"fixture").expect("socket fixture");
        fs::set_permissions(handle.socket_path(), fs::Permissions::from_mode(0o600))
            .expect("socket permissions");
        handle.shutdown().await.expect("shutdown");
        let status = std::process::Command::new("kill")
            .args(["-0", descendant.trim()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("probe descendant");
        assert!(!status.success(), "descendant process survived group kill");
        assert!(!handle.socket_path().exists());
        fs::remove_dir_all(stable_root).expect("cleanup");
    }

    #[tokio::test]
    async fn shutdown_forces_a_worker_that_ignores_termination() {
        let mut spec = shell_spec("forced-shutdown", "unused");
        spec.graceful_shutdown_timeout = Duration::from_millis(50);
        let runtime = spec.runtime_dir.clone();
        let ready = runtime.join("ready");
        spec.args = vec![
            "-c".to_string(),
            format!("trap '' TERM; : > {}; sleep 60", ready.display()),
        ];
        let handle = ManagedWorkerHandle::spawn(spec).await.expect("spawn");
        for _ in 0..100 {
            if ready.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(ready.exists(), "worker did not install its TERM handler");
        let started = Instant::now();
        let exit = handle.shutdown().await.expect("forced shutdown");
        assert!(started.elapsed() >= Duration::from_millis(50));
        assert!(!exit.success);
        assert_eq!(handle.try_wait().await.expect("cached exit"), Some(exit));
        fs::remove_dir_all(runtime).expect("cleanup");
    }
}
