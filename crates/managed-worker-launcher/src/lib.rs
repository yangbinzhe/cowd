//! Product-neutral, fail-closed Linux exec boundary for managed workers.

#![cfg_attr(test, allow(clippy::expect_used, clippy::field_reassign_with_default))]

use std::{
    collections::{BTreeMap, BTreeSet},
    convert::TryInto,
    ffi::OsString,
    fs,
    io::{self, Read, Write},
    os::{
        fd::AsRawFd,
        unix::{
            ffi::OsStrExt,
            fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
            net::UnixStream,
            process::CommandExt,
        },
    },
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use landlock::{
    Access, AccessFs, PathBeneath, PathFd, Ruleset, RulesetAttr, RulesetCreatedAttr, RulesetStatus,
    ABI,
};
use seccompiler::{
    BpfProgram, SeccompAction, SeccompCmpArgLen, SeccompCmpOp, SeccompCondition, SeccompFilter,
    SeccompRule,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const LAUNCH_SCHEMA_VERSION_V1: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationModeV1 {
    Enforce,
    Relaxed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceLimitsV1 {
    pub nofile: u64,
    pub nproc: u64,
    pub address_space_bytes: u64,
    pub cpu_seconds: u64,
    pub file_size_bytes: u64,
    pub cgroup_memory_bytes: u64,
    pub cgroup_pids: u64,
    pub cgroup_cpu_quota_us: u64,
    pub cgroup_cpu_period_us: u64,
}

impl Default for ResourceLimitsV1 {
    fn default() -> Self {
        Self {
            nofile: 256,
            nproc: 4096,
            address_space_bytes: 512 * 1024 * 1024,
            cpu_seconds: 300,
            file_size_bytes: 16 * 1024 * 1024,
            cgroup_memory_bytes: 512 * 1024 * 1024,
            cgroup_pids: 64,
            cgroup_cpu_quota_us: 100_000,
            cgroup_cpu_period_us: 100_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryPolicyV1 {
    pub runtime_dir: PathBuf,
    pub data_dir: PathBuf,
    pub config_dir: PathBuf,
    pub bundle_dir: PathBuf,
    pub read_only_dirs: Vec<PathBuf>,
    pub runtime_read_only_dirs: Vec<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkPolicyV1 {
    UnixOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerIsolationPolicyV1 {
    pub mode: IsolationModeV1,
    pub directories: DirectoryPolicyV1,
    pub limits: ResourceLimitsV1,
    pub cgroup_path: Option<PathBuf>,
    pub network: NetworkPolicyV1,
}

impl WorkerIsolationPolicyV1 {
    pub fn validate(&self) -> Result<(), LauncherError> {
        for value in [
            self.limits.nofile,
            self.limits.nproc,
            self.limits.address_space_bytes,
            self.limits.cpu_seconds,
            self.limits.file_size_bytes,
            self.limits.cgroup_memory_bytes,
            self.limits.cgroup_pids,
            self.limits.cgroup_cpu_quota_us,
            self.limits.cgroup_cpu_period_us,
        ] {
            if value == 0 {
                return Err(LauncherError::InvalidSpec(
                    "resource limits must be positive".into(),
                ));
            }
        }
        validate_directories(&self.directories)?;
        if self.mode == IsolationModeV1::Enforce && self.cgroup_path.is_none() {
            return Err(LauncherError::IsolationUnavailable(
                "delegated cgroup v2 path is required".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchProtocolV1 {
    pub schema_version: u32,
    pub launch_id: String,
    pub gateway_instance: String,
    pub generation: String,
    pub parent_pid: u32,
    pub parent_boot_id: String,
    pub launcher_sha256: String,
    pub target_path: PathBuf,
    pub target_sha256: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub allowed_env_keys: BTreeSet<String>,
    pub isolation: WorkerIsolationPolicyV1,
    pub status_socket: PathBuf,
    pub identity_path: PathBuf,
    pub deadline_unix_ms: u64,
}

impl LaunchProtocolV1 {
    pub fn validate(&self) -> Result<(), LauncherError> {
        if self.schema_version != LAUNCH_SCHEMA_VERSION_V1 {
            return Err(LauncherError::InvalidSpec(
                "unsupported schema_version".into(),
            ));
        }
        for (name, value) in [
            ("launch_id", self.launch_id.as_str()),
            ("gateway_instance", self.gateway_instance.as_str()),
            ("generation", self.generation.as_str()),
        ] {
            if value.trim().is_empty() || value.len() > 256 || value.contains('\0') {
                return Err(LauncherError::InvalidSpec(format!("invalid {name}")));
            }
        }
        if now_ms() > self.deadline_unix_ms {
            return Err(LauncherError::DeadlineExpired);
        }
        if !self.target_path.is_absolute()
            || !self.status_socket.is_absolute()
            || !self.identity_path.is_absolute()
        {
            return Err(LauncherError::InvalidSpec(
                "target, status and identity paths must be absolute".into(),
            ));
        }
        if self
            .env
            .keys()
            .any(|key| !self.allowed_env_keys.contains(key))
        {
            return Err(LauncherError::InvalidSpec(
                "environment contains a key outside allowed_env_keys".into(),
            ));
        }
        if self
            .allowed_env_keys
            .iter()
            .any(|key| key.is_empty() || key.contains('=') || key.contains('\0'))
        {
            return Err(LauncherError::InvalidSpec("invalid environment key".into()));
        }
        for value in self.env.values() {
            if value.contains('\0') {
                return Err(LauncherError::InvalidSpec(
                    "environment contains NUL".into(),
                ));
            }
        }
        self.isolation.validate()?;
        let runtime = fs::canonicalize(&self.isolation.directories.runtime_dir)
            .map_err(|error| io_error(&self.isolation.directories.runtime_dir, error))?;
        if self.status_socket.parent() != Some(runtime.as_path())
            || self.identity_path.parent() != Some(runtime.as_path())
        {
            return Err(LauncherError::InvalidSpec(
                "status socket and identity must be direct children of runtime_dir".into(),
            ));
        }
        if self.isolation.mode == IsolationModeV1::Enforce
            && !self
                .target_path
                .starts_with(&self.isolation.directories.bundle_dir)
        {
            return Err(LauncherError::InvalidSpec(
                "enforced target must be contained by bundle_dir".into(),
            ));
        }
        Ok(())
    }

    pub fn digest(&self) -> Result<String, LauncherError> {
        let bytes = serde_json::to_vec(self)
            .map_err(|error| LauncherError::InvalidSpec(error.to_string()))?;
        Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerIdentityV1 {
    pub schema_version: u32,
    pub pid: u32,
    pub pgid: u32,
    pub proc_start_ticks: u64,
    pub boot_id: String,
    pub target_path: PathBuf,
    pub target_sha256: String,
    pub generation: String,
    pub gateway_instance: String,
    pub launch_id: String,
    pub launch_digest: String,
    pub cgroup_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KernelReceiptV1 {
    pub schema_version: u32,
    pub launch_id: String,
    pub pid: u32,
    pub proc_start_ticks: u64,
    pub launch_digest: String,
    pub landlock_abi: Option<u8>,
    pub landlock_enforced: bool,
    pub seccomp_unix_only: bool,
    pub cgroup_enforced: bool,
    pub rlimits_enforced: bool,
    pub inherited_fds_cloexec: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum LauncherError {
    #[error("invalid launch specification: {0}")]
    InvalidSpec(String),
    #[error("launch specification deadline expired")]
    DeadlineExpired,
    #[error("launcher I/O failed for {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("launcher isolation unavailable: {0}")]
    IsolationUnavailable(String),
    #[error("launcher parent identity changed")]
    ParentChanged,
    #[error("launcher target digest mismatch")]
    TargetDigestMismatch,
    #[error("launcher exec failed: {0}")]
    Exec(String),
}

fn io_error(path: impl Into<PathBuf>, source: io::Error) -> LauncherError {
    LauncherError::Io {
        path: path.into(),
        source,
    }
}

pub fn run_from_args(args: impl IntoIterator<Item = OsString>) -> Result<(), LauncherError> {
    let mut args = args.into_iter();
    let spec_path = args
        .next()
        .ok_or_else(|| LauncherError::InvalidSpec("launch spec path is required".into()))?;
    if args.next().is_some() {
        return Err(LauncherError::InvalidSpec(
            "unexpected launcher argument".into(),
        ));
    }
    run(Path::new(&spec_path))
}

pub fn run(spec_path: &Path) -> Result<(), LauncherError> {
    let spec = consume_spec(spec_path)?;
    verify_parent(&spec)?;
    install_parent_death_signal(spec.parent_pid)?;
    spec.validate()?;
    verify_launcher(&spec)?;
    verify_target(&spec)?;
    install_no_new_privileges()?;
    if spec.isolation.mode == IsolationModeV1::Enforce {
        install_rlimits(&spec.isolation.limits)?;
    }
    if let Some(cgroup) = &spec.isolation.cgroup_path {
        if spec.isolation.mode == IsolationModeV1::Enforce && !is_cgroup2_mount(cgroup) {
            return Err(LauncherError::IsolationUnavailable(format!(
                "cgroup path is not on a cgroup v2 mount: {}",
                cgroup.display()
            )));
        }
        fs::write(cgroup.join("cgroup.procs"), std::process::id().to_string())
            .map_err(|error| io_error(cgroup.join("cgroup.procs"), error))?;
    }
    let mut status = UnixStream::connect(&spec.status_socket)
        .map_err(|error| io_error(&spec.status_socket, error))?;
    close_inherited_fds(status.as_raw_fd())?;
    let identity = capture_identity(&spec)?;
    write_identity(&spec.identity_path, &identity)?;
    let mut receipt = KernelReceiptV1 {
        schema_version: LAUNCH_SCHEMA_VERSION_V1,
        launch_id: spec.launch_id.clone(),
        pid: identity.pid,
        proc_start_ticks: identity.proc_start_ticks,
        launch_digest: identity.launch_digest.clone(),
        landlock_abi: None,
        landlock_enforced: false,
        seccomp_unix_only: false,
        cgroup_enforced: spec.isolation.cgroup_path.is_some(),
        rlimits_enforced: spec.isolation.mode == IsolationModeV1::Enforce,
        inherited_fds_cloexec: true,
    };
    if spec.isolation.mode == IsolationModeV1::Enforce {
        receipt.landlock_abi = Some(install_landlock(&spec.isolation.directories)?);
        receipt.landlock_enforced = true;
        install_seccomp()?;
        receipt.seccomp_unix_only = true;
    }
    serde_json::to_writer(&mut status, &receipt)
        .map_err(|error| LauncherError::InvalidSpec(error.to_string()))?;
    status
        .write_all(b"\n")
        .map_err(|error| io_error(&spec.status_socket, error))?;
    status
        .flush()
        .map_err(|error| io_error(&spec.status_socket, error))?;

    let error = Command::new(&spec.target_path)
        .args(&spec.args)
        .env_clear()
        .envs(&spec.env)
        .exec();
    let _ = writeln!(status, "{{\"exec_error\":{:?}}}", error.to_string());
    Err(LauncherError::Exec(error.to_string()))
}

fn consume_spec(path: &Path) -> Result<LaunchProtocolV1, LauncherError> {
    let mut file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|error| io_error(path, error))?;
    let metadata = file.metadata().map_err(|error| io_error(path, error))?;
    let mode = metadata.permissions().mode() & 0o777;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || mode != 0o600
        || metadata.uid() != effective_uid()
    {
        return Err(LauncherError::InvalidSpec(format!(
            "launch spec must be a 0600 regular file owned by the launcher uid; mode={mode:o}"
        )));
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|error| io_error(path, error))?;
    fs::remove_file(path).map_err(|error| io_error(path, error))?;
    serde_json::from_slice(&bytes).map_err(|error| LauncherError::InvalidSpec(error.to_string()))
}

fn validate_directories(policy: &DirectoryPolicyV1) -> Result<(), LauncherError> {
    let runtime = real_directory("runtime", &policy.runtime_dir)?;
    let data = real_directory("data", &policy.data_dir)?;
    let _config = real_directory("config", &policy.config_dir)?;
    let bundle = real_directory("bundle", &policy.bundle_dir)?;
    for path in &policy.read_only_dirs {
        real_directory("read-only", path)?;
    }
    let mut runtime_roots = BTreeSet::new();
    for path in &policy.runtime_read_only_dirs {
        let canonical = real_directory("runtime read-only", path)?;
        let metadata =
            fs::symlink_metadata(&canonical).map_err(|error| io_error(&canonical, error))?;
        if canonical == Path::new("/")
            || canonical != *path
            || metadata.mode() & 0o002 != 0
            || (effective_uid() != 0 && metadata.uid() == effective_uid())
            || !runtime_roots.insert(canonical.clone())
        {
            return Err(LauncherError::InvalidSpec(format!(
                "runtime read-only root must be canonical, unique, bounded, and not writable by the worker uid: {}",
                path.display()
            )));
        }
        for ancestor in canonical
            .ancestors()
            .skip(1)
            .filter(|ancestor| *ancestor != Path::new("/"))
        {
            let metadata =
                fs::symlink_metadata(ancestor).map_err(|error| io_error(ancestor, error))?;
            if metadata.file_type().is_symlink() || metadata.mode() & 0o002 != 0 {
                return Err(LauncherError::InvalidSpec(format!(
                    "runtime read-only root has an unsafe ancestor: {}",
                    ancestor.display()
                )));
            }
        }
    }
    for writable in [&runtime, &data] {
        if writable == Path::new("/") || bundle.starts_with(writable) {
            return Err(LauncherError::InvalidSpec(format!(
                "writable directory {} contains bundle {}",
                writable.display(),
                bundle.display()
            )));
        }
    }
    Ok(())
}

fn real_directory(name: &str, path: &Path) -> Result<PathBuf, LauncherError> {
    if !path.is_absolute() {
        return Err(LauncherError::InvalidSpec(format!(
            "{name} directory is relative"
        )));
    }
    let metadata = fs::symlink_metadata(path).map_err(|error| io_error(path, error))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(LauncherError::InvalidSpec(format!(
            "{name} is not a real directory"
        )));
    }
    fs::canonicalize(path).map_err(|error| io_error(path, error))
}

fn verify_parent(spec: &LaunchProtocolV1) -> Result<(), LauncherError> {
    if parent_pid() != spec.parent_pid || read_boot_id()? != spec.parent_boot_id {
        return Err(LauncherError::ParentChanged);
    }
    Ok(())
}

fn verify_target(spec: &LaunchProtocolV1) -> Result<(), LauncherError> {
    let canonical =
        fs::canonicalize(&spec.target_path).map_err(|error| io_error(&spec.target_path, error))?;
    if canonical != spec.target_path || sha256_file(&canonical)? != spec.target_sha256 {
        return Err(LauncherError::TargetDigestMismatch);
    }
    Ok(())
}

fn verify_launcher(spec: &LaunchProtocolV1) -> Result<(), LauncherError> {
    let running =
        fs::canonicalize("/proc/self/exe").map_err(|error| io_error("/proc/self/exe", error))?;
    if sha256_file(&running)? != spec.launcher_sha256 {
        return Err(LauncherError::TargetDigestMismatch);
    }
    Ok(())
}

fn capture_identity(spec: &LaunchProtocolV1) -> Result<WorkerIdentityV1, LauncherError> {
    let pid = std::process::id();
    let (pgid, start_ticks) = proc_identity(pid)?;
    Ok(WorkerIdentityV1 {
        schema_version: LAUNCH_SCHEMA_VERSION_V1,
        pid,
        pgid,
        proc_start_ticks: start_ticks,
        boot_id: read_boot_id()?,
        target_path: spec.target_path.clone(),
        target_sha256: spec.target_sha256.clone(),
        generation: spec.generation.clone(),
        gateway_instance: spec.gateway_instance.clone(),
        launch_id: spec.launch_id.clone(),
        launch_digest: spec.digest()?,
        cgroup_path: spec.isolation.cgroup_path.clone(),
    })
}

fn write_identity(path: &Path, identity: &WorkerIdentityV1) -> Result<(), LauncherError> {
    let parent = path
        .parent()
        .ok_or_else(|| LauncherError::InvalidSpec("identity has no parent".into()))?;
    let temporary = path.with_extension("tmp");
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)
        .map_err(|error| io_error(&temporary, error))?;
    serde_json::to_writer(&mut file, identity)
        .map_err(|error| LauncherError::InvalidSpec(error.to_string()))?;
    file.sync_all()
        .map_err(|error| io_error(&temporary, error))?;
    fs::rename(&temporary, path).map_err(|error| io_error(path, error))?;
    fs::File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(|error| io_error(parent, error))
}

fn install_parent_death_signal(expected_parent: u32) -> Result<(), LauncherError> {
    // SAFETY: prctl mutates only this launcher process; arguments match PR_SET_PDEATHSIG.
    if unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) } != 0 {
        return Err(LauncherError::IsolationUnavailable(
            io::Error::last_os_error().to_string(),
        ));
    }
    if parent_pid() != expected_parent {
        return Err(LauncherError::ParentChanged);
    }
    Ok(())
}

fn install_no_new_privileges() -> Result<(), LauncherError> {
    // SAFETY: prctl mutates only this launcher process; trailing arguments are zero as required.
    if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } == 0 {
        Ok(())
    } else {
        Err(LauncherError::IsolationUnavailable(
            io::Error::last_os_error().to_string(),
        ))
    }
}

fn install_rlimits(limits: &ResourceLimitsV1) -> Result<(), LauncherError> {
    for (resource, value) in [
        (libc::RLIMIT_NOFILE, limits.nofile),
        (libc::RLIMIT_NPROC, limits.nproc),
        (libc::RLIMIT_AS, limits.address_space_bytes),
        (libc::RLIMIT_CPU, limits.cpu_seconds),
        (libc::RLIMIT_FSIZE, limits.file_size_bytes),
        (libc::RLIMIT_CORE, 0),
    ] {
        let limit = libc::rlimit {
            rlim_cur: value,
            rlim_max: value,
        };
        // SAFETY: setrlimit reads the initialized limit and affects only this process.
        if unsafe { libc::setrlimit(resource, &limit) } != 0 {
            return Err(LauncherError::IsolationUnavailable(
                io::Error::last_os_error().to_string(),
            ));
        }
    }
    Ok(())
}

fn close_inherited_fds(status_fd: i32) -> Result<(), LauncherError> {
    let entries =
        fs::read_dir("/proc/self/fd").map_err(|error| io_error("/proc/self/fd", error))?;
    for entry in entries {
        let entry = entry.map_err(|error| io_error("/proc/self/fd", error))?;
        let Some(fd) = entry
            .file_name()
            .to_str()
            .and_then(|value| value.parse::<i32>().ok())
        else {
            continue;
        };
        if fd <= 2 || fd == status_fd {
            continue;
        }
        // SAFETY: F_GETFD/F_SETFD do not take pointer arguments and operate on an observed open fd.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if flags >= 0 && unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } != 0 {
            return Err(LauncherError::IsolationUnavailable(
                io::Error::last_os_error().to_string(),
            ));
        }
    }
    Ok(())
}

fn install_landlock(policy: &DirectoryPolicyV1) -> Result<u8, LauncherError> {
    let abi = ABI::V5;
    let all = AccessFs::from_all(abi);
    let read = AccessFs::from_read(abi);
    let mut ruleset = Ruleset::default()
        .handle_access(all)
        .map_err(|error| LauncherError::IsolationUnavailable(error.to_string()))?
        .create()
        .map_err(|error| LauncherError::IsolationUnavailable(error.to_string()))?;
    // APP workers bind their ready identity to their own executable, uid,
    // parent and cgroup. Pinning the caller's procfs inode keeps those reads
    // available after exec without exposing /proc or another process.
    ruleset = ruleset
        .add_rule(PathBeneath::new(
            PathFd::new("/proc/self")
                .map_err(|error| LauncherError::IsolationUnavailable(error.to_string()))?,
            read,
        ))
        .map_err(|error| LauncherError::IsolationUnavailable(error.to_string()))?;
    for root in std::iter::once(&policy.config_dir)
        .chain(std::iter::once(&policy.bundle_dir))
        .chain(policy.read_only_dirs.iter())
        .chain(policy.runtime_read_only_dirs.iter())
    {
        ruleset = ruleset
            .add_rule(PathBeneath::new(
                PathFd::new(root)
                    .map_err(|error| LauncherError::IsolationUnavailable(error.to_string()))?,
                read,
            ))
            .map_err(|error| LauncherError::IsolationUnavailable(error.to_string()))?;
    }
    for root in [&policy.runtime_dir, &policy.data_dir] {
        ruleset = ruleset
            .add_rule(PathBeneath::new(
                PathFd::new(root)
                    .map_err(|error| LauncherError::IsolationUnavailable(error.to_string()))?,
                all,
            ))
            .map_err(|error| LauncherError::IsolationUnavailable(error.to_string()))?;
    }
    let status = ruleset
        .restrict_self()
        .map_err(|error| LauncherError::IsolationUnavailable(error.to_string()))?;
    if status.ruleset == RulesetStatus::FullyEnforced && status.no_new_privs {
        match status.landlock {
            landlock::LandlockStatus::Available { effective_abi, .. } => Ok(effective_abi as u8),
            other => Err(LauncherError::IsolationUnavailable(format!(
                "Landlock unavailable after restriction: {other:?}"
            ))),
        }
    } else {
        Err(LauncherError::IsolationUnavailable(format!(
            "Landlock status: {status:?}"
        )))
    }
}

fn install_seccomp() -> Result<(), LauncherError> {
    let non_unix = SeccompRule::new(vec![SeccompCondition::new(
        0,
        SeccompCmpArgLen::Dword,
        SeccompCmpOp::Ne,
        libc::AF_UNIX as u64,
    )
    .map_err(|error| LauncherError::IsolationUnavailable(error.to_string()))?])
    .map_err(|error| LauncherError::IsolationUnavailable(error.to_string()))?;
    let foreign_tgid = SeccompRule::new(vec![SeccompCondition::new(
        0,
        SeccompCmpArgLen::Dword,
        SeccompCmpOp::Ne,
        u64::from(std::process::id()),
    )
    .map_err(|error| LauncherError::IsolationUnavailable(error.to_string()))?])
    .map_err(|error| LauncherError::IsolationUnavailable(error.to_string()))?;
    let mut rules = BTreeMap::from([
        (libc::SYS_socket, vec![non_unix.clone()]),
        (libc::SYS_socketpair, vec![non_unix]),
        (libc::SYS_tgkill, vec![foreign_tgid]),
    ]);
    for syscall in [
        libc::SYS_kill,
        libc::SYS_tkill,
        libc::SYS_ptrace,
        libc::SYS_process_vm_readv,
        libc::SYS_process_vm_writev,
        libc::SYS_pidfd_open,
        libc::SYS_pidfd_getfd,
        libc::SYS_pidfd_send_signal,
        libc::SYS_io_uring_setup,
        libc::SYS_bpf,
        libc::SYS_keyctl,
        libc::SYS_mount,
        libc::SYS_umount2,
        libc::SYS_open_by_handle_at,
        libc::SYS_kexec_load,
        libc::SYS_setpriority,
    ] {
        rules.insert(syscall, Vec::new());
    }
    let filter: BpfProgram = SeccompFilter::new(
        rules,
        SeccompAction::Allow,
        SeccompAction::Errno(libc::EPERM as u32),
        std::env::consts::ARCH.try_into().map_err(|error| {
            LauncherError::IsolationUnavailable(format!("architecture: {error}"))
        })?,
    )
    .map_err(|error| LauncherError::IsolationUnavailable(error.to_string()))?
    .try_into()
    .map_err(|error: seccompiler::BackendError| {
        LauncherError::IsolationUnavailable(error.to_string())
    })?;
    seccompiler::apply_filter(&filter)
        .map_err(|error| LauncherError::IsolationUnavailable(error.to_string()))
}

pub fn sha256_file(path: &Path) -> Result<String, LauncherError> {
    let mut file = fs::File::open(path).map_err(|error| io_error(path, error))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| io_error(path, error))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

#[must_use]
pub fn is_cgroup2_mount(path: &Path) -> bool {
    let Ok(path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    let mut info = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: path is NUL-terminated and info points to writable, properly aligned storage.
    if unsafe { libc::statfs(path.as_ptr(), info.as_mut_ptr()) } != 0 {
        return false;
    }
    // SAFETY: successful statfs initialized the full output structure.
    let info = unsafe { info.assume_init() };
    info.f_type as libc::c_long == libc::CGROUP2_SUPER_MAGIC
}

pub fn read_boot_id() -> Result<String, LauncherError> {
    let path = Path::new("/proc/sys/kernel/random/boot_id");
    Ok(fs::read_to_string(path)
        .map_err(|error| io_error(path, error))?
        .trim()
        .to_string())
}

pub fn proc_identity(pid: u32) -> Result<(u32, u64), LauncherError> {
    let path = PathBuf::from(format!("/proc/{pid}/stat"));
    let stat = fs::read_to_string(&path).map_err(|error| io_error(&path, error))?;
    let close = stat
        .rfind(')')
        .ok_or_else(|| LauncherError::InvalidSpec("malformed proc stat".into()))?;
    let fields: Vec<&str> = stat[close + 1..].split_whitespace().collect();
    if fields.len() <= 19 {
        return Err(LauncherError::InvalidSpec("short proc stat".into()));
    }
    let pgid = fields[2]
        .parse()
        .map_err(|error| LauncherError::InvalidSpec(format!("pgid: {error}")))?;
    let ticks = fields[19]
        .parse()
        .map_err(|error| LauncherError::InvalidSpec(format!("start ticks: {error}")))?;
    Ok((pgid, ticks))
}

fn effective_uid() -> u32 {
    // SAFETY: geteuid has no arguments and no memory-safety preconditions.
    unsafe { libc::geteuid() }
}

fn parent_pid() -> u32 {
    // SAFETY: getppid has no arguments and no memory-safety preconditions.
    unsafe { libc::getppid() as u32 }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Stdio;

    fn root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "managed-worker-launcher-{label}-{}-{}",
            std::process::id(),
            now_ms()
        ))
    }

    fn protocol(root: &Path) -> LaunchProtocolV1 {
        for name in ["runtime", "data", "config", "bundle"] {
            fs::create_dir_all(root.join(name)).expect("fixture directory");
        }
        let target = fs::canonicalize("/bin/true").expect("target");
        LaunchProtocolV1 {
            schema_version: 1,
            launch_id: "launch-1".into(),
            gateway_instance: "gateway-1".into(),
            generation: "generation-1".into(),
            parent_pid: parent_pid(),
            parent_boot_id: read_boot_id().expect("boot id"),
            launcher_sha256: sha256_file(&std::env::current_exe().expect("launcher path"))
                .expect("launcher digest"),
            target_sha256: sha256_file(&target).expect("target digest"),
            target_path: target,
            args: Vec::new(),
            env: BTreeMap::new(),
            allowed_env_keys: BTreeSet::new(),
            isolation: WorkerIsolationPolicyV1 {
                mode: IsolationModeV1::Relaxed,
                directories: DirectoryPolicyV1 {
                    runtime_dir: root.join("runtime"),
                    data_dir: root.join("data"),
                    config_dir: root.join("config"),
                    bundle_dir: root.join("bundle"),
                    read_only_dirs: Vec::new(),
                    runtime_read_only_dirs: Vec::new(),
                },
                limits: ResourceLimitsV1::default(),
                cgroup_path: None,
                network: NetworkPolicyV1::UnixOnly,
            },
            status_socket: root.join("runtime/status.sock"),
            identity_path: root.join("runtime/identity.json"),
            deadline_unix_ms: now_ms() + 10_000,
        }
    }

    #[test]
    fn strict_protocol_rejects_unknown_fields_and_ambiguous_write_roots() {
        let bad = br#"{"schema_version":1,"unknown":true}"#;
        assert!(serde_json::from_slice::<LaunchProtocolV1>(bad).is_err());
    }

    #[test]
    fn runtime_read_only_roots_reject_root_duplicates_user_writable_and_symlink_paths() {
        let root = root("runtime-roots");
        let mut directories = protocol(&root).isolation.directories;
        let system = fs::canonicalize("/usr/lib").expect("system library root");
        directories.runtime_read_only_dirs = vec![system.clone()];
        validate_directories(&directories).expect("canonical system runtime root");

        directories.runtime_read_only_dirs = vec![PathBuf::from("/")];
        assert!(matches!(
            validate_directories(&directories),
            Err(LauncherError::InvalidSpec(_))
        ));
        directories.runtime_read_only_dirs = vec![system.clone(), system];
        assert!(matches!(
            validate_directories(&directories),
            Err(LauncherError::InvalidSpec(_))
        ));
        directories.runtime_read_only_dirs = vec![root.join("outside")];
        fs::create_dir_all(root.join("outside")).expect("writable directory");
        assert!(matches!(
            validate_directories(&directories),
            Err(LauncherError::InvalidSpec(_))
        ));
        let alias = root.join("runtime-root-alias");
        std::os::unix::fs::symlink("/usr/lib", &alias).expect("runtime root symlink");
        directories.runtime_read_only_dirs = vec![alias];
        assert!(matches!(
            validate_directories(&directories),
            Err(LauncherError::InvalidSpec(_))
        ));
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn proc_identity_and_hash_are_real() {
        let (pgid, ticks) = proc_identity(std::process::id()).expect("proc identity");
        assert!(pgid > 0);
        assert!(ticks > 0);
        let executable = std::env::current_exe().expect("current exe");
        assert!(sha256_file(&executable)
            .expect("digest")
            .starts_with("sha256:"));
    }

    #[test]
    fn launch_spec_is_owner_only_single_use_and_target_tamper_is_rejected() {
        let root = root("spec");
        let spec = protocol(&root);
        let path = root.join("runtime/spec.json");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .expect("spec file");
        serde_json::to_writer(&mut file, &spec).expect("encode");
        drop(file);
        assert_eq!(consume_spec(&path).expect("consume"), spec);
        assert!(matches!(consume_spec(&path), Err(LauncherError::Io { .. })));

        let permissive = root.join("runtime/permissive.json");
        fs::write(&permissive, serde_json::to_vec(&spec).expect("encode")).expect("write");
        fs::set_permissions(&permissive, fs::Permissions::from_mode(0o644)).expect("mode");
        assert!(matches!(
            consume_spec(&permissive),
            Err(LauncherError::InvalidSpec(_))
        ));

        let mut tampered = spec;
        tampered.target_sha256 = "sha256:00".into();
        assert!(matches!(
            verify_target(&tampered),
            Err(LauncherError::TargetDigestMismatch)
        ));
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn real_kernel_landlock_seccomp_and_rlimit_probe() {
        let root = root("kernel-parent");
        for name in ["runtime", "data", "config", "bundle", "outside"] {
            fs::create_dir_all(root.join(name)).expect("fixture directory");
        }
        fs::write(root.join("config/readable"), b"config").expect("config fixture");
        fs::write(root.join("bundle/readable"), b"bundle").expect("bundle fixture");
        fs::copy("/bin/true", root.join("bundle/dynamic-worker")).expect("dynamic worker fixture");
        fs::write(root.join("outside/secret"), b"secret").expect("outside fixture");
        let status = Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", "tests::kernel_probe_child", "--nocapture"])
            .env("COWD_KERNEL_PROBE_ROOT", &root)
            .stdin(Stdio::null())
            .status()
            .expect("kernel probe child");
        assert!(status.success(), "kernel probe child failed: {status}");
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn kernel_probe_child() {
        let Some(root) = std::env::var_os("COWD_KERNEL_PROBE_ROOT").map(PathBuf::from) else {
            return;
        };
        install_no_new_privileges().expect("no_new_privs");
        let inherited = fs::File::open("/dev/null").expect("fd fixture");
        close_inherited_fds(-1).expect("fd closure");
        // SAFETY: F_GETFD has no pointer arguments and observes this valid open descriptor.
        let fd_flags = unsafe { libc::fcntl(inherited.as_raw_fd(), libc::F_GETFD) };
        assert_ne!(fd_flags & libc::FD_CLOEXEC, 0);
        let mut limits = ResourceLimitsV1::default();
        limits.nofile = 64;
        limits.nproc = 1024;
        limits.address_space_bytes = 1024 * 1024 * 1024;
        install_rlimits(&limits).expect("rlimits");
        let mut system_runtime_roots = ["/lib", "/lib64", "/usr/lib", "/usr/lib64"]
            .into_iter()
            .filter(|path| Path::new(path).exists())
            .map(|path| fs::canonicalize(path).expect("canonical system runtime root"))
            .collect::<Vec<_>>();
        system_runtime_roots.sort();
        system_runtime_roots.dedup();
        let policy = DirectoryPolicyV1 {
            runtime_dir: root.join("runtime"),
            data_dir: root.join("data"),
            config_dir: root.join("config"),
            bundle_dir: root.join("bundle"),
            read_only_dirs: vec![PathBuf::from("/dev")],
            runtime_read_only_dirs: system_runtime_roots,
        };
        let abi = install_landlock(&policy).expect("Landlock fully enforced");
        assert!(abi > 0);
        install_seccomp().expect("seccomp");
        let dynamic = Command::new(root.join("bundle/dynamic-worker"))
            .status()
            .expect("dynamic ELF starts under enforced Landlock and Unix-only seccomp");
        assert!(dynamic.success());
        fs::write(root.join("runtime/ok"), b"ok").expect("runtime writable");
        fs::write(root.join("data/ok"), b"ok").expect("data writable");
        assert!(fs::write(root.join("config/denied"), b"no").is_err());
        assert!(fs::write(root.join("bundle/denied"), b"no").is_err());
        assert!(fs::write(root.join("outside/denied"), b"no").is_err());
        assert_eq!(
            fs::read(root.join("config/readable")).expect("config readable"),
            b"config"
        );
        assert_eq!(
            fs::read(root.join("bundle/readable")).expect("bundle readable"),
            b"bundle"
        );
        assert!(fs::read_to_string("/proc/self/stat").is_ok());
        assert!(fs::read_to_string("/proc/self/status").is_ok());
        assert!(fs::read_to_string("/proc/self/cgroup").is_ok());
        assert!(fs::read_link("/proc/self/exe").is_ok());
        if std::process::id() != 1 {
            assert!(fs::read_to_string("/proc/1/stat").is_err());
        }
        assert!(fs::read(root.join("outside/secret")).is_err());
        let (_left, _right) = std::os::unix::net::UnixStream::pair().expect("AF_UNIX allowed");
        // SAFETY: socket has scalar arguments and returns a new descriptor or -1.
        let inet = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0) };
        assert_eq!(inet, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EPERM));
        // SAFETY: kill with signal 0 has no side effect; seccomp must reject the foreign PID.
        assert_eq!(unsafe { libc::kill(parent_pid() as i32, 0) }, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EPERM));
        let mut observed = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: getrlimit writes to initialized, valid storage.
        assert_eq!(
            unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut observed) },
            0
        );
        assert_eq!(observed.rlim_cur, 64);
        let opened: Vec<_> = (0..128)
            .map_while(|_| fs::File::open("/dev/null").ok())
            .collect();
        assert!(opened.len() < 64, "RLIMIT_NOFILE did not cap descriptors");
        let mut allocation = Vec::<u8>::new();
        assert!(allocation
            .try_reserve_exact(2 * 1024 * 1024 * 1024)
            .is_err());
    }

    #[test]
    fn parent_death_signal_closes_the_spawn_window() {
        let root = root("pdeath");
        fs::create_dir_all(&root).expect("fixture");
        let pid_file = root.join("worker.pid");
        let status = Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", "tests::pdeath_parent_child", "--nocapture"])
            .env("COWD_PDEATH_PID_FILE", &pid_file)
            .stdin(Stdio::null())
            .status()
            .expect("parent helper");
        assert!(status.success());
        let pid: u32 = fs::read_to_string(&pid_file)
            .expect("pid file")
            .trim()
            .parse()
            .expect("pid");
        for _ in 0..100 {
            if !Path::new(&format!("/proc/{pid}")).exists() {
                fs::remove_dir_all(root).expect("cleanup");
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("launcher child survived its parent");
    }

    #[test]
    #[allow(clippy::zombie_processes)]
    fn pdeath_parent_child() {
        let Some(pid_file) = std::env::var_os("COWD_PDEATH_PID_FILE") else {
            return;
        };
        let mut child = Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", "tests::pdeath_worker_child", "--nocapture"])
            .env("COWD_PDEATH_WORKER_FILE", &pid_file)
            .spawn()
            .expect("worker helper");
        for _ in 0..100 {
            if Path::new(&pid_file).exists() {
                return;
            }
            if child.try_wait().expect("worker status").is_some() {
                panic!("worker helper exited before arming pdeathsig");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("worker helper did not arm pdeathsig");
    }

    #[test]
    fn pdeath_worker_child() {
        let Some(pid_file) = std::env::var_os("COWD_PDEATH_WORKER_FILE") else {
            return;
        };
        let expected = parent_pid();
        install_parent_death_signal(expected).expect("pdeathsig");
        fs::write(pid_file, std::process::id().to_string()).expect("pid evidence");
        std::thread::sleep(std::time::Duration::from_secs(60));
    }

    #[test]
    fn default_limits_keep_uid_scope_and_worker_scope_separate() {
        let limits = ResourceLimitsV1::default();
        assert_eq!(limits.nproc, 4096);
        assert_eq!(limits.address_space_bytes, 512 * 1024 * 1024);
        assert_eq!(limits.cgroup_pids, 64);
    }
}
