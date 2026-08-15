use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    os::unix::{
        fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
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
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    net::UnixListener,
    process::{Child, Command},
    sync::Mutex,
    task::JoinHandle,
    time::Instant,
};

use crate::{
    log_buffer::BoundedLogBuffer, CancellationToken, CredentialLease, CredentialSecret,
    GenerationFence, LogSnapshot, ManagedWorkerError, ManagedWorkerResult, WorkerRuntimeDir,
};
use managed_worker_launcher::{
    read_boot_id, sha256_file, DirectoryPolicyV1, IsolationModeV1, KernelReceiptV1,
    LaunchProtocolV1, NetworkPolicyV1, ResourceLimitsV1, WorkerIdentityV1, WorkerIsolationPolicyV1,
    LAUNCH_SCHEMA_VERSION_V1,
};

const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// A worker launch specification.
#[cfg_attr(
    not(feature = "test-support"),
    doc = r#"
Direct process execution is deliberately absent from normal builds:

```compile_fail
use managed_worker_runtime::ManagedWorkerSpec;

let _ = ManagedWorkerSpec::new("/bin/true", "/tmp/worker", "generation-1")
    .direct_test_process();
```
"#
)]
#[derive(Debug, Clone)]
pub struct ManagedWorkerSpec {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub allowed_env_keys: BTreeSet<String>,
    pub runtime_dir: PathBuf,
    pub generation: String,
    pub startup_timeout: Duration,
    pub graceful_shutdown_timeout: Duration,
    pub log_capacity_bytes: usize,
    pub require_socket: bool,
    pub socket_env: Option<String>,
    pub credential_env: Option<String>,
    pub generation_env: Option<String>,
    pub launcher_path: PathBuf,
    pub launcher_sha256: String,
    pub gateway_instance: String,
    pub data_dir: PathBuf,
    pub config_dir: PathBuf,
    pub bundle_dir: PathBuf,
    pub read_only_dirs: Vec<PathBuf>,
    pub isolation_mode: IsolationModeV1,
    pub resource_limits: ResourceLimitsV1,
    pub cgroup_root: Option<PathBuf>,
    #[cfg(feature = "test-support")]
    direct_test_process: bool,
    #[cfg(test)]
    test_launcher_entry: bool,
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
            allowed_env_keys: BTreeSet::new(),
            runtime_dir: runtime_dir.into(),
            generation: generation.into(),
            startup_timeout: Duration::from_secs(10),
            graceful_shutdown_timeout: Duration::from_secs(5),
            log_capacity_bytes: 256 * 1024,
            require_socket: true,
            socket_env: None,
            credential_env: None,
            generation_env: None,
            launcher_path: PathBuf::new(),
            launcher_sha256: String::new(),
            gateway_instance: String::new(),
            data_dir: PathBuf::new(),
            config_dir: PathBuf::new(),
            bundle_dir: PathBuf::new(),
            read_only_dirs: Vec::new(),
            isolation_mode: IsolationModeV1::Enforce,
            resource_limits: ResourceLimitsV1::default(),
            cgroup_root: None,
            #[cfg(feature = "test-support")]
            direct_test_process: false,
            #[cfg(test)]
            test_launcher_entry: false,
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

    #[must_use]
    pub fn allow_env_key(mut self, key: impl Into<String>) -> Self {
        self.allowed_env_keys.insert(key.into());
        self
    }

    #[must_use]
    pub fn launcher(mut self, path: impl Into<PathBuf>, sha256: impl Into<String>) -> Self {
        self.launcher_path = path.into();
        self.launcher_sha256 = sha256.into();
        self
    }

    #[must_use]
    pub fn gateway_instance(mut self, value: impl Into<String>) -> Self {
        self.gateway_instance = value.into();
        self
    }

    #[must_use]
    pub fn directories(
        mut self,
        data: impl Into<PathBuf>,
        config: impl Into<PathBuf>,
        bundle: impl Into<PathBuf>,
    ) -> Self {
        self.data_dir = data.into();
        self.config_dir = config.into();
        self.bundle_dir = bundle.into();
        self
    }

    #[must_use]
    pub fn relaxed_for_tests(mut self) -> Self {
        self.isolation_mode = IsolationModeV1::Relaxed;
        self
    }

    /// Explicitly selects the unisolated process kernel used by cross-crate tests.
    ///
    /// This method and its backing state do not exist unless the non-default
    /// `test-support` feature is enabled.
    #[cfg(feature = "test-support")]
    #[must_use]
    pub fn direct_test_process(mut self) -> Self {
        self.direct_test_process = true;
        self
    }

    fn validate(&self) -> ManagedWorkerResult<()> {
        self.validate_common()?;
        #[cfg(feature = "test-support")]
        if self.direct_test_process {
            return Ok(());
        }
        self.validate_isolated()
    }

    fn validate_common(&self) -> ManagedWorkerResult<()> {
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
        if self
            .env
            .keys()
            .any(|key| !self.allowed_env_keys.contains(key))
        {
            return Err(ManagedWorkerError::InvalidSpec(
                "environment contains a key outside the explicit allowlist".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_isolated(&self) -> ManagedWorkerResult<()> {
        if self.launcher_path.as_os_str().is_empty()
            || self.launcher_sha256.is_empty()
            || self.gateway_instance.trim().is_empty()
        {
            return Err(ManagedWorkerError::InvalidSpec(
                "launcher path/digest and gateway instance are required".to_string(),
            ));
        }
        if self.isolation_mode == IsolationModeV1::Enforce && self.cgroup_root.is_none() {
            return Err(ManagedWorkerError::InvalidSpec(
                "enforced isolation requires a delegated cgroup v2 root".to_string(),
            ));
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
    identity: Option<WorkerIdentityV1>,
    cgroup_dir: Option<PathBuf>,
    closed: AtomicBool,
}

impl ManagedWorkerHandle {
    pub async fn spawn(spec: ManagedWorkerSpec) -> ManagedWorkerResult<Self> {
        spec.validate()?;
        let generation = GenerationFence::new(spec.generation.clone())?;
        let runtime = WorkerRuntimeDir::create(&spec.runtime_dir)?;
        if runtime.identity_path().exists() {
            return Err(ManagedWorkerError::InvalidSpec(
                "worker identity already exists; runtime recovery is required before spawn"
                    .to_string(),
            ));
        }
        runtime.cleanup_ephemeral()?;
        let mut runtime_guard = RuntimeSpawnGuard::new(runtime.clone());
        let (credential, bootstrap_secret) = CredentialLease::create(&runtime)?;

        #[cfg(feature = "test-support")]
        if spec.direct_test_process {
            return Self::spawn_direct_test_process(
                spec,
                runtime,
                runtime_guard,
                generation,
                credential,
                bootstrap_secret,
            )
            .await;
        }

        let launcher_path = fs::canonicalize(&spec.launcher_path)
            .map_err(|error| ManagedWorkerError::io(&spec.launcher_path, error))?;
        if sha256_file(&launcher_path)
            .map_err(|error| ManagedWorkerError::Launcher(error.to_string()))?
            != spec.launcher_sha256
        {
            return Err(ManagedWorkerError::Launcher(
                "launcher artifact digest mismatch".to_string(),
            ));
        }
        let target_path = fs::canonicalize(&spec.program)
            .map_err(|error| ManagedWorkerError::io(&spec.program, error))?;
        let target_sha256 = sha256_file(&target_path)
            .map_err(|error| ManagedWorkerError::Launcher(error.to_string()))?;
        let launch_id = format!("{:016x}", rand::random::<u64>());
        let mut cgroup_guard = prepare_cgroup(&spec, &launch_id)?;
        let status_listener = UnixListener::bind(runtime.status_socket_path())
            .map_err(|error| ManagedWorkerError::io(runtime.status_socket_path(), error))?;
        fs::set_permissions(
            runtime.status_socket_path(),
            fs::Permissions::from_mode(0o600),
        )
        .map_err(|error| ManagedWorkerError::io(runtime.status_socket_path(), error))?;

        let mut env = spec.env.clone();
        let mut allowed_env_keys = spec.allowed_env_keys.clone();
        if let Some(name) = &spec.socket_env {
            env.insert(name.clone(), runtime.socket_path().display().to_string());
            allowed_env_keys.insert(name.clone());
        }
        if let Some(name) = &spec.credential_env {
            env.insert(name.clone(), credential.path().display().to_string());
            allowed_env_keys.insert(name.clone());
        }
        if let Some(name) = &spec.generation_env {
            env.insert(name.clone(), generation.as_str().to_string());
            allowed_env_keys.insert(name.clone());
        }
        let launch = LaunchProtocolV1 {
            schema_version: LAUNCH_SCHEMA_VERSION_V1,
            launch_id: launch_id.clone(),
            gateway_instance: spec.gateway_instance.clone(),
            generation: generation.as_str().to_string(),
            parent_pid: std::process::id(),
            parent_boot_id: read_boot_id()
                .map_err(|error| ManagedWorkerError::Launcher(error.to_string()))?,
            launcher_sha256: spec.launcher_sha256.clone(),
            target_path: target_path.clone(),
            target_sha256: target_sha256.clone(),
            args: spec.args.clone(),
            allowed_env_keys,
            env,
            isolation: WorkerIsolationPolicyV1 {
                mode: spec.isolation_mode,
                directories: DirectoryPolicyV1 {
                    runtime_dir: runtime.root().to_path_buf(),
                    data_dir: spec.data_dir.clone(),
                    config_dir: spec.config_dir.clone(),
                    bundle_dir: spec.bundle_dir.clone(),
                    read_only_dirs: spec.read_only_dirs.clone(),
                },
                limits: spec.resource_limits.clone(),
                cgroup_path: cgroup_guard.path().map(Path::to_path_buf),
                network: NetworkPolicyV1::UnixOnly,
            },
            status_socket: runtime.status_socket_path(),
            identity_path: runtime.identity_path(),
            deadline_unix_ms: unix_now_ms().saturating_add(
                u64::try_from(spec.startup_timeout.as_millis()).unwrap_or(u64::MAX),
            ),
        };
        write_launch_spec(&runtime.launch_spec_path(), &launch)?;

        let mut command = Command::new(&launcher_path);
        command.env_clear();
        #[cfg(test)]
        if spec.test_launcher_entry {
            command
                .args(["--quiet", "--exact", "process::tests::launcher_entry"])
                .env("COWD_TEST_LAUNCH_SPEC", runtime.launch_spec_path());
        } else {
            command.arg(runtime.launch_spec_path());
        }
        #[cfg(not(test))]
        command.arg(runtime.launch_spec_path());
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command.as_std_mut().process_group(0);
        let mut child = command
            .spawn()
            .map_err(|error| ManagedWorkerError::io(&launcher_path, error))?;
        let pid = child.id().ok_or_else(|| {
            ManagedWorkerError::InvalidSpec("spawned worker has no process id".to_string())
        })?;
        let stdout = BoundedLogBuffer::new(spec.log_capacity_bytes);
        let stderr = BoundedLogBuffer::new(spec.log_capacity_bytes);
        let mut drain_tasks = Vec::with_capacity(2);
        if let Some(pipe) = child.stdout.take() {
            let buffer = stdout.clone();
            #[cfg(test)]
            let task = if spec.test_launcher_entry {
                tokio::spawn(async move { buffer.drain_skipping(pipe, 16).await })
            } else {
                tokio::spawn(async move { buffer.drain(pipe).await })
            };
            #[cfg(not(test))]
            let task = tokio::spawn(async move { buffer.drain(pipe).await });
            drain_tasks.push(task);
        }
        if let Some(pipe) = child.stderr.take() {
            let buffer = stderr.clone();
            drain_tasks.push(tokio::spawn(async move { buffer.drain(pipe).await }));
        }
        let identity =
            match await_launcher_exec(&status_listener, &launch, pid, spec.startup_timeout).await {
                Ok(identity) => identity,
                Err(error) => {
                    let _ = signal_process_group(pid, "KILL").await;
                    let _ = child.wait().await;
                    let _ = runtime.cleanup_ephemeral();
                    let _ = fs::remove_file(runtime.identity_path());
                    return Err(error);
                }
            };
        let cgroup_dir = cgroup_guard.release();
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
                identity: Some(identity),
                cgroup_dir,
                closed: AtomicBool::new(false),
            }),
        };
        runtime_guard.release();
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

    #[cfg(feature = "test-support")]
    async fn spawn_direct_test_process(
        spec: ManagedWorkerSpec,
        runtime: WorkerRuntimeDir,
        mut runtime_guard: RuntimeSpawnGuard,
        generation: GenerationFence,
        credential: CredentialLease,
        bootstrap_secret: CredentialSecret,
    ) -> ManagedWorkerResult<Self> {
        let mut command = Command::new(&spec.program);
        command.env_clear().args(&spec.args);
        for (key, value) in &spec.env {
            command.env(key, value);
        }
        if let Some(name) = &spec.socket_env {
            command.env(name, runtime.socket_path());
        }
        if let Some(name) = &spec.credential_env {
            command.env(name, credential.path());
        }
        if let Some(name) = &spec.generation_env {
            command.env(name, generation.as_str());
        }
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command.as_std_mut().process_group(0);
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
                identity: None,
                cgroup_dir: None,
                closed: AtomicBool::new(false),
            }),
        };
        runtime_guard.release();
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

    #[must_use]
    #[cfg(not(feature = "test-support"))]
    pub fn identity(&self) -> &WorkerIdentityV1 {
        let Some(identity) = self.inner.identity.as_ref() else {
            unreachable!("normal builds create only launcher-verified worker handles");
        };
        identity
    }

    /// Returns no launcher identity for an explicitly selected direct test process.
    #[must_use]
    #[cfg(feature = "test-support")]
    pub fn identity(&self) -> Option<&WorkerIdentityV1> {
        self.inner.identity.as_ref()
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
        let _ = fs::remove_file(self.inner.runtime.identity_path());
        cleanup_cgroup(self.inner.cgroup_dir.as_deref());
    }
}

impl Drop for ManagedWorkerInner {
    fn drop(&mut self) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let _ = std::process::Command::new("/bin/kill")
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
        let _ = fs::remove_file(self.runtime.identity_path());
        cleanup_cgroup(self.cgroup_dir.as_deref());
    }
}

async fn signal_process_group(pid: u32, signal: &str) -> ManagedWorkerResult<()> {
    let status = Command::new("/bin/kill")
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

fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[derive(Debug)]
struct RuntimeSpawnGuard {
    runtime: WorkerRuntimeDir,
    armed: bool,
}

impl RuntimeSpawnGuard {
    fn new(runtime: WorkerRuntimeDir) -> Self {
        Self {
            runtime,
            armed: true,
        }
    }

    fn release(&mut self) {
        self.armed = false;
    }
}

impl Drop for RuntimeSpawnGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.runtime.cleanup_ephemeral();
            let _ = fs::remove_file(self.runtime.identity_path());
        }
    }
}

fn write_launch_spec(path: &Path, launch: &LaunchProtocolV1) -> ManagedWorkerResult<()> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|error| ManagedWorkerError::io(path, error))?;
    serde_json::to_writer(&mut file, launch)
        .map_err(|error| ManagedWorkerError::Launcher(error.to_string()))?;
    file.flush()
        .and_then(|()| file.sync_all())
        .map_err(|error| ManagedWorkerError::io(path, error))
}

#[derive(Debug)]
struct CgroupSpawnGuard {
    path: Option<PathBuf>,
}

impl CgroupSpawnGuard {
    fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    fn release(&mut self) -> Option<PathBuf> {
        self.path.take()
    }
}

impl Drop for CgroupSpawnGuard {
    fn drop(&mut self) {
        cleanup_cgroup(self.path.as_deref());
    }
}

fn prepare_cgroup(
    spec: &ManagedWorkerSpec,
    launch_id: &str,
) -> ManagedWorkerResult<CgroupSpawnGuard> {
    if spec.isolation_mode == IsolationModeV1::Relaxed {
        return Ok(CgroupSpawnGuard { path: None });
    }
    let root = spec.cgroup_root.as_ref().ok_or_else(|| {
        ManagedWorkerError::Launcher("delegated cgroup v2 root is absent".to_string())
    })?;
    if !managed_worker_launcher::is_cgroup2_mount(root)
        || !root.join("cgroup.controllers").is_file()
        || !root.join("cgroup.procs").is_file()
    {
        return Err(ManagedWorkerError::Launcher(
            "cgroup root is not a v2 hierarchy".to_string(),
        ));
    }
    let path = root.join(format!("worker-{launch_id}"));
    fs::create_dir(&path).map_err(|error| ManagedWorkerError::io(&path, error))?;
    for (name, value) in [
        (
            "memory.max",
            spec.resource_limits.cgroup_memory_bytes.to_string(),
        ),
        ("pids.max", spec.resource_limits.cgroup_pids.to_string()),
        (
            "cpu.max",
            format!(
                "{} {}",
                spec.resource_limits.cgroup_cpu_quota_us, spec.resource_limits.cgroup_cpu_period_us
            ),
        ),
    ] {
        let target = path.join(name);
        if let Err(error) = fs::write(&target, value) {
            let _ = fs::remove_dir(&path);
            return Err(ManagedWorkerError::io(target, error));
        }
    }
    Ok(CgroupSpawnGuard { path: Some(path) })
}

fn cleanup_cgroup(path: Option<&Path>) {
    if let Some(path) = path {
        let _ = fs::remove_dir(path);
    }
}

async fn await_launcher_exec(
    listener: &UnixListener,
    launch: &LaunchProtocolV1,
    pid: u32,
    timeout: Duration,
) -> ManagedWorkerResult<WorkerIdentityV1> {
    let exchange = async {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(|error| ManagedWorkerError::io(&launch.status_socket, error))?;
        let peer = stream
            .peer_cred()
            .map_err(|error| ManagedWorkerError::io(&launch.status_socket, error))?;
        let expected_peer = i32::try_from(pid).map_err(|error| {
            ManagedWorkerError::Launcher(format!("launcher pid is out of range: {error}"))
        })?;
        if peer.pid() != Some(expected_peer) {
            return Err(ManagedWorkerError::Launcher(format!(
                "status peer pid {:?} did not match launcher pid {pid}",
                peer.pid()
            )));
        }
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .await
            .map_err(|error| ManagedWorkerError::io(&launch.status_socket, error))?;
        let receipt: KernelReceiptV1 = serde_json::from_str(line.trim_end()).map_err(|error| {
            ManagedWorkerError::Launcher(format!("invalid kernel receipt: {error}"))
        })?;
        if receipt.schema_version != LAUNCH_SCHEMA_VERSION_V1
            || receipt.launch_id != launch.launch_id
            || receipt.pid != pid
            || receipt.launch_digest
                != launch
                    .digest()
                    .map_err(|error| ManagedWorkerError::Launcher(error.to_string()))?
            || !receipt.inherited_fds_cloexec
            || (launch.isolation.mode == IsolationModeV1::Enforce
                && (!receipt.rlimits_enforced
                    || !receipt.landlock_enforced
                    || !receipt.seccomp_unix_only
                    || !receipt.cgroup_enforced))
        {
            return Err(ManagedWorkerError::Launcher(
                "kernel receipt did not match the admitted launch".to_string(),
            ));
        }
        let mut trailing = Vec::new();
        reader
            .read_to_end(&mut trailing)
            .await
            .map_err(|error| ManagedWorkerError::io(&launch.status_socket, error))?;
        if !trailing.is_empty() {
            return Err(ManagedWorkerError::Launcher(format!(
                "target exec failed: {}",
                String::from_utf8_lossy(&trailing)
            )));
        }
        let metadata = fs::symlink_metadata(&launch.identity_path)
            .map_err(|error| ManagedWorkerError::io(&launch.identity_path, error))?;
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.permissions().mode() & 0o777 != 0o600
        {
            return Err(ManagedWorkerError::Launcher(
                "worker identity is not a 0600 regular file".to_string(),
            ));
        }
        let identity: WorkerIdentityV1 = serde_json::from_slice(
            &fs::read(&launch.identity_path)
                .map_err(|error| ManagedWorkerError::io(&launch.identity_path, error))?,
        )
        .map_err(|error| {
            ManagedWorkerError::Launcher(format!("invalid worker identity: {error}"))
        })?;
        if identity.pid != pid
            || identity.proc_start_ticks != receipt.proc_start_ticks
            || identity.launch_digest != receipt.launch_digest
            || identity.target_path != launch.target_path
            || identity.target_sha256 != launch.target_sha256
        {
            return Err(ManagedWorkerError::Launcher(
                "worker identity did not match the kernel receipt".to_string(),
            ));
        }
        let observed_exe = fs::read_link(format!("/proc/{pid}/exe"))
            .map_err(|error| ManagedWorkerError::io(format!("/proc/{pid}/exe"), error))?;
        let (_, observed_ticks) = managed_worker_launcher::proc_identity(pid)
            .map_err(|error| ManagedWorkerError::Launcher(error.to_string()))?;
        if observed_exe != launch.target_path || observed_ticks != identity.proc_start_ticks {
            return Err(ManagedWorkerError::Launcher(
                "post-exec /proc identity did not match the target".to_string(),
            ));
        }
        Ok(identity)
    };
    tokio::time::timeout(timeout, exchange)
        .await
        .map_err(|_| ManagedWorkerError::DeadlineExceeded(timeout))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, os::unix::fs::PermissionsExt};
    use tokio::io::AsyncWriteExt;

    fn temp_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "managed-worker-process-{label}-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ))
    }

    fn launcher_path() -> PathBuf {
        std::env::current_exe().expect("test executable")
    }

    fn configure_test_spec(mut spec: ManagedWorkerSpec) -> ManagedWorkerSpec {
        let runtime = spec.runtime_dir.clone();
        let data = runtime.join("data");
        let config = runtime.join("config");
        let bundle = runtime.with_extension("bundle");
        for path in [&runtime, &data, &config, &bundle] {
            fs::create_dir_all(path).expect("test directory");
        }
        let launcher = launcher_path();
        let digest = sha256_file(&launcher).expect("launcher digest");
        spec.launcher_path = launcher;
        spec.launcher_sha256 = digest;
        spec.gateway_instance = "test-gateway".to_string();
        spec.data_dir = data;
        spec.config_dir = config;
        spec.bundle_dir = bundle;
        spec.isolation_mode = IsolationModeV1::Relaxed;
        spec.test_launcher_entry = true;
        spec
    }

    #[test]
    fn launcher_entry() {
        let Some(path) = std::env::var_os("COWD_TEST_LAUNCH_SPEC") else {
            return;
        };
        match managed_worker_launcher::run(Path::new(&path)) {
            Ok(()) => std::process::exit(0),
            Err(error) => {
                eprintln!("test launcher failed: {error}");
                std::process::exit(125);
            }
        }
    }

    fn shell_spec(label: &str, script: &str) -> ManagedWorkerSpec {
        let mut spec = configure_test_spec(
            ManagedWorkerSpec::new("/bin/sh", temp_path(label), "generation-1")
                .args(["-c", script]),
        );
        spec.require_socket = false;
        spec.startup_timeout = Duration::from_secs(2);
        spec.graceful_shutdown_timeout = Duration::from_millis(150);
        spec.log_capacity_bytes = 1024;
        spec
    }

    #[cfg(not(feature = "test-support"))]
    #[tokio::test]
    async fn default_build_rejects_a_spec_without_the_isolated_launcher() {
        let root = temp_path("missing-launcher");
        let spec = ManagedWorkerSpec::new("/bin/true", &root, "generation-1");
        assert!(matches!(
            ManagedWorkerHandle::spawn(spec).await,
            Err(ManagedWorkerError::InvalidSpec(message))
                if message == "launcher path/digest and gateway instance are required"
        ));
        assert!(!root.exists());
    }

    #[tokio::test]
    async fn existing_runtime_identity_is_never_overwritten_by_spawn() {
        let root = temp_path("identity-collision");
        let spec = configure_test_spec(ManagedWorkerSpec::new(
            "/bin/true",
            &root,
            "generation-collision",
        ));
        let identity = root.join("worker-identity.json");
        let socket = root.join("w.sock");
        fs::write(&socket, b"foreign socket").expect("foreign socket");
        fs::write(&identity, b"foreign-identity").expect("foreign identity");
        fs::set_permissions(&identity, fs::Permissions::from_mode(0o600))
            .expect("foreign identity mode");
        assert!(matches!(
            ManagedWorkerHandle::spawn(spec).await,
            Err(ManagedWorkerError::InvalidSpec(message))
                if message == "worker identity already exists; runtime recovery is required before spawn"
        ));
        assert_eq!(
            fs::read(&identity).expect("preserved identity"),
            b"foreign-identity"
        );
        assert_eq!(
            fs::read(&socket).expect("preserved socket"),
            b"foreign socket"
        );
        fs::remove_dir_all(root).expect("cleanup collision fixture");
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
        let mut spec = configure_test_spec(
            ManagedWorkerSpec::new("/bin/sh", &root, "generation-1").args(["-c", "sleep 60"]),
        );
        spec.startup_timeout = Duration::from_millis(50);
        spec.graceful_shutdown_timeout = Duration::from_millis(100);
        assert!(matches!(
            ManagedWorkerHandle::spawn(spec).await,
            Err(ManagedWorkerError::DeadlineExceeded(_))
        ));
        assert!(!root.join("w.sock").exists());
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
        let mut spec = configure_test_spec(
            ManagedWorkerSpec::new("/usr/bin/python3", &root, "generation-1").args(["-c", script]),
        );
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
        spec.allowed_env_keys.insert("PATH".to_string());
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

    #[tokio::test]
    async fn enforced_mode_rejects_a_fake_cgroup_hierarchy_before_spawn() {
        let mut spec = shell_spec("fake-cgroup", "sleep 60");
        spec.isolation_mode = IsolationModeV1::Enforce;
        let fake = spec.runtime_dir.join("fake-cgroup");
        fs::create_dir_all(&fake).expect("fake hierarchy");
        fs::write(fake.join("cgroup.controllers"), b"cpu memory pids").expect("controllers");
        fs::write(fake.join("cgroup.procs"), b"").expect("procs");
        spec.cgroup_root = Some(fake);
        assert!(matches!(
            ManagedWorkerHandle::spawn(spec).await,
            Err(ManagedWorkerError::Launcher(message)) if message.contains("not a v2 hierarchy")
        ));
    }

    #[tokio::test]
    async fn launcher_artifact_tamper_is_rejected_before_spawn() {
        let mut spec = shell_spec("tampered-launcher", "sleep 60");
        spec.launcher_sha256 = "sha256:00".to_string();
        assert!(matches!(
            ManagedWorkerHandle::spawn(spec).await,
            Err(ManagedWorkerError::Launcher(message)) if message.contains("digest mismatch")
        ));
    }

    #[tokio::test]
    async fn forged_status_receipt_is_rejected() {
        let spec = shell_spec("forged-status", "sleep 60");
        let runtime = WorkerRuntimeDir::create(&spec.runtime_dir).expect("runtime");
        let listener = UnixListener::bind(runtime.status_socket_path()).expect("status listener");
        let target = fs::canonicalize(&spec.program).expect("target");
        let launch = LaunchProtocolV1 {
            schema_version: 1,
            launch_id: "real-launch".into(),
            gateway_instance: "test-gateway".into(),
            generation: "generation-1".into(),
            parent_pid: std::process::id(),
            parent_boot_id: read_boot_id().expect("boot id"),
            launcher_sha256: sha256_file(&std::env::current_exe().expect("launcher path"))
                .expect("launcher digest"),
            target_sha256: sha256_file(&target).expect("digest"),
            target_path: target,
            args: Vec::new(),
            env: BTreeMap::new(),
            allowed_env_keys: BTreeSet::new(),
            isolation: WorkerIsolationPolicyV1 {
                mode: IsolationModeV1::Relaxed,
                directories: DirectoryPolicyV1 {
                    runtime_dir: runtime.root().to_path_buf(),
                    data_dir: spec.data_dir,
                    config_dir: spec.config_dir,
                    bundle_dir: spec.bundle_dir,
                    read_only_dirs: Vec::new(),
                },
                limits: ResourceLimitsV1::default(),
                cgroup_path: None,
                network: NetworkPolicyV1::UnixOnly,
            },
            status_socket: runtime.status_socket_path(),
            identity_path: runtime.identity_path(),
            deadline_unix_ms: unix_now_ms() + 1_000,
        };
        let socket = runtime.status_socket_path();
        tokio::spawn(async move {
            let mut stream = tokio::net::UnixStream::connect(socket)
                .await
                .expect("forge connect");
            let forged = KernelReceiptV1 {
                schema_version: 1,
                launch_id: "forged-launch".into(),
                pid: std::process::id(),
                proc_start_ticks: 1,
                launch_digest: "sha256:forged".into(),
                landlock_abi: None,
                landlock_enforced: false,
                seccomp_unix_only: false,
                cgroup_enforced: false,
                rlimits_enforced: false,
                inherited_fds_cloexec: true,
            };
            let mut bytes = serde_json::to_vec(&forged).expect("forge encode");
            bytes.push(b'\n');
            stream.write_all(&bytes).await.expect("forge write");
        });
        assert!(matches!(
            await_launcher_exec(
                &listener,
                &launch,
                std::process::id(),
                Duration::from_secs(1)
            )
            .await,
            Err(ManagedWorkerError::Launcher(message)) if message.contains("did not match")
        ));
        fs::remove_dir_all(runtime.root()).expect("cleanup");
    }
}
