//! Rootless process launcher for untrusted tools and managed sidecars.
//!
//! This crate deliberately has no unsafe Rust and no host-process fallback.
//! It builds a Bubblewrap command which exposes only declared filesystem
//! roots, clears the environment twice (host command and Bubblewrap), closes
//! inherited descriptors before `bwrap` executes, and performs a real Linux
//! deny probe before every launch.
//!
//! `bwrap` provides the namespace and mount boundary. The trusted inner
//! launcher then installs Landlock and seccomp before executing untrusted code.

use std::{
    collections::{BTreeSet, HashSet},
    env, fs,
    io::Read,
    path::{Component, Path, PathBuf},
    process::{Command, ExitCode, Output, Stdio},
    sync::atomic::{AtomicBool, Ordering},
};

#[cfg(target_os = "linux")]
use std::{collections::BTreeMap, convert::TryInto};

#[cfg(target_os = "linux")]
use landlock::{
    Access, AccessFs, PathBeneath, PathFd, Ruleset, RulesetAttr, RulesetCreatedAttr, ABI,
};
#[cfg(target_os = "linux")]
use seccompiler::{
    BpfProgram, SeccompAction, SeccompCmpArgLen, SeccompCmpOp, SeccompCondition, SeccompFilter,
    SeccompRule,
};
use thiserror::Error;

const SYSTEM_READ_ONLY_ROOTS: &[&str] = &["/usr", "/bin", "/lib", "/lib64", "/etc"];
const SANDBOX_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
const BOOTSTRAP_SHELL: &str = "/bin/bash";
const INNER_LAUNCHER_PATH: &str = "/run/cowd-sandbox-inner";
const LAUNCHER_PROTOCOL: &str = concat!(
    "sandbox-launcher/",
    env!("CARGO_PKG_VERSION"),
    "/kernel-v3-controlled-host"
);
const INTERNAL_DISPATCH: &str = "__cowd_internal";
const INTERNAL_ROLE: &str = "sandbox-launcher";
static COWD_PROCESS_HOST: AtomicBool = AtomicBool::new(false);

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

/// 标记当前进程由唯一的 `cowd` 可执行文件启动。
///
/// 生产主入口必须在任何 Gateway、Runtime 或工具初始化前调用。这样沙箱
/// 可直接复用 `current_exe`，无需为同一文件重复启动协议探测子进程。
#[doc(hidden)]
pub fn register_cowd_process_host() {
    COWD_PROCESS_HOST.store(true, Ordering::Release);
}

/// 构造一个固定到当前 Cowd 运行映像的内部子进程命令。
///
/// Linux 的 `/proc/self/exe` 在 `exec` 发生前始终引用调用进程当前映像，
/// 因此磁盘上的 `cowd` 被原子替换时不会混入新版本。
#[doc(hidden)]
pub fn cowd_internal_process_command() -> Result<Command, String> {
    if !COWD_PROCESS_HOST.load(Ordering::Acquire) {
        return Err("Cowd process host identity was not registered".to_string());
    }
    platform_internal_process_command()
}

#[cfg(target_os = "linux")]
fn platform_internal_process_command() -> Result<Command, String> {
    use std::os::unix::process::CommandExt;

    let display_path =
        env::current_exe().map_err(|error| format!("failed to locate Cowd executable: {error}"))?;
    fs::metadata("/proc/self/exe")
        .map_err(|error| format!("failed to inspect running Cowd executable: {error}"))?;
    let mut command = Command::new("/proc/self/exe");
    command.arg0(display_path);
    Ok(command)
}

#[cfg(not(target_os = "linux"))]
fn platform_internal_process_command() -> Result<Command, String> {
    Err("Cowd internal process hosting requires Linux executable identity pinning".to_string())
}

/// 运行 Cowd 单文件架构中的沙箱子进程角色。
///
/// 外层模式只用于诊断和真实验收；生产工具链通常把同一个 `cowd` 文件
/// 只读挂载进 bwrap，再通过 `--inner` 安装 Landlock 与 seccomp。
#[doc(hidden)]
pub fn internal_process_entry(args: &[String]) -> ExitCode {
    #[cfg(target_os = "linux")]
    if args.first().map(String::as_str) == Some("--stream-outer") {
        return bounded::stream_outer_entry(&args[1..]);
    }
    #[cfg(target_os = "linux")]
    if args.first().map(String::as_str) == Some("--stream-inner") {
        return bounded::stream_inner_entry(&args[1..]);
    }
    #[cfg(target_os = "linux")]
    if args.first().map(String::as_str) == Some("--bounded-outer") {
        return bounded::outer_entry(&args[1..]);
    }
    #[cfg(target_os = "linux")]
    if args.first().map(String::as_str) == Some("--bounded-inner") {
        return bounded::inner_entry(&args[1..]);
    }
    #[cfg(target_os = "linux")]
    if args.first().map(String::as_str) == Some("--bounded-child") {
        return bounded::child_entry(&args[1..]);
    }
    if args.first().map(String::as_str) == Some("--protocol-version") {
        println!("{LAUNCHER_PROTOCOL}");
        return ExitCode::SUCCESS;
    }
    if args.first().map(String::as_str) == Some("--inner") {
        return inner_process_entry(&args[1..]);
    }
    let Some(workspace) = args.first() else {
        eprintln!(
            "usage: cowd __cowd_internal sandbox-launcher <absolute-workspace> <shell-command>"
        );
        return ExitCode::from(64);
    };
    let command = args[1..].join(" ");
    if command.trim().is_empty() {
        eprintln!("shell command is required");
        return ExitCode::from(64);
    }
    let spec = SandboxLaunchSpec::workspace(workspace);
    match shell_command(&command, &spec) {
        Ok(prepared) => match prepared.into_command().status() {
            Ok(status) => ExitCode::from(status.code().unwrap_or(1) as u8),
            Err(error) => {
                eprintln!("failed to launch sandbox: {error}");
                ExitCode::from(1)
            }
        },
        Err(error) => {
            eprintln!("sandbox unavailable: {error}");
            ExitCode::from(1)
        }
    }
}

#[cfg(target_os = "linux")]
fn inner_process_entry(args: &[String]) -> ExitCode {
    let mut workspace = None;
    let mut workspace_access = SandboxWorkspaceAccess::ReadWrite;
    let mut network_isolated = false;
    let mut writable = Vec::new();
    let mut command = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--workspace" if index + 1 < args.len() => {
                workspace = Some(PathBuf::from(&args[index + 1]));
                index += 2;
            }
            "--workspace-read-only" => {
                workspace_access = SandboxWorkspaceAccess::ReadOnly;
                index += 1;
            }
            "--network-isolated" => {
                network_isolated = true;
                index += 1;
            }
            "--writable" if index + 1 < args.len() => {
                writable.push(PathBuf::from(&args[index + 1]));
                index += 2;
            }
            "--" if index + 1 < args.len() => {
                command = Some(args[index + 1..].join(" "));
                break;
            }
            _ => return ExitCode::from(64),
        }
    }
    let Some(workspace) = workspace else {
        eprintln!("sandbox inner launcher requires --workspace");
        return ExitCode::from(64);
    };
    let Some(command) = command else {
        eprintln!("sandbox inner launcher requires a command after --");
        return ExitCode::from(64);
    };
    if workspace_access == SandboxWorkspaceAccess::ReadWrite {
        writable.push(workspace);
    }
    writable.push(PathBuf::from("/tmp"));
    writable.push(PathBuf::from("/dev"));
    if let Err(error) = install_landlock(&writable) {
        eprintln!("failed to install Landlock: {error}");
        return ExitCode::from(125);
    }
    if let Err(error) = install_seccomp(network_isolated) {
        eprintln!("failed to install seccomp: {error}");
        return ExitCode::from(125);
    }
    match Command::new("/bin/sh").arg("-c").arg(command).status() {
        Ok(status) => ExitCode::from(status.code().unwrap_or(1) as u8),
        Err(error) => {
            eprintln!("failed to execute hardened sandbox command: {error}");
            ExitCode::from(125)
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn inner_process_entry(_args: &[String]) -> ExitCode {
    eprintln!("sandbox inner launcher is only supported on Linux");
    ExitCode::from(125)
}

#[cfg(target_os = "linux")]
fn install_landlock(writable: &[PathBuf]) -> Result<(), String> {
    let abi = ABI::V5;
    let access_all = AccessFs::from_all(abi);
    let access_read = AccessFs::from_read(abi);
    let mut ruleset = Ruleset::default()
        .handle_access(access_all)
        .map_err(|error| error.to_string())?
        .create()
        .map_err(|error| error.to_string())?
        .add_rule(PathBeneath::new(
            PathFd::new(Path::new("/")).map_err(|error| error.to_string())?,
            access_read,
        ))
        .map_err(|error| error.to_string())?;
    for root in writable.iter().filter(|root| root.exists()) {
        ruleset = ruleset
            .add_rule(PathBeneath::new(
                PathFd::new(root).map_err(|error| error.to_string())?,
                access_all,
            ))
            .map_err(|error| error.to_string())?;
    }
    let status = ruleset.restrict_self().map_err(|error| error.to_string())?;
    let rendered = format!("{status:?}");
    if !rendered.contains("FullyEnforced") || !rendered.contains("no_new_privs: true") {
        return Err(format!("Landlock was not fully enforced: {rendered}"));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn install_seccomp(network_isolated: bool) -> Result<(), String> {
    let syscalls = [
        libc::SYS_ptrace,
        libc::SYS_process_vm_readv,
        libc::SYS_process_vm_writev,
        libc::SYS_keyctl,
        libc::SYS_open_by_handle_at,
        libc::SYS_mount,
        libc::SYS_umount2,
        libc::SYS_bpf,
        libc::SYS_kexec_load,
    ];
    let rules = syscalls
        .into_iter()
        .map(|syscall| (syscall, Vec::new()))
        .collect::<BTreeMap<_, _>>();
    let filter: BpfProgram = SeccompFilter::new(
        rules,
        SeccompAction::Allow,
        SeccompAction::Trap,
        std::env::consts::ARCH
            .try_into()
            .map_err(|error| format!("unsupported seccomp architecture: {error}"))?,
    )
    .map_err(|error| error.to_string())?
    .try_into()
    .map_err(|error: seccompiler::BackendError| error.to_string())?;
    seccompiler::apply_filter(&filter).map_err(|error| error.to_string())?;
    if network_isolated {
        let filter = isolated_network_filter()?;
        seccompiler::apply_filter(&filter).map_err(|error| error.to_string())?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn isolated_network_filter() -> Result<BpfProgram, String> {
    let not_equal = |argument, value: i32| {
        SeccompCondition::new(
            argument,
            SeccompCmpArgLen::Dword,
            SeccompCmpOp::Ne,
            value as u32 as u64,
        )
        .map_err(|error| error.to_string())
    };
    let socket_rule = SeccompRule::new(vec![
        not_equal(0, libc::AF_INET)?,
        not_equal(0, libc::AF_INET6)?,
    ])
    .map_err(|error| error.to_string())?;
    let mut invalid_pair_types = Vec::new();
    for socket_type in [libc::SOCK_STREAM, libc::SOCK_SEQPACKET] {
        for flags in [
            0,
            libc::SOCK_CLOEXEC,
            libc::SOCK_NONBLOCK,
            libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
        ] {
            invalid_pair_types.push(not_equal(1, socket_type | flags)?);
        }
    }
    let pair_rules = vec![
        SeccompRule::new(vec![not_equal(0, libc::AF_UNIX)?]).map_err(|error| error.to_string())?,
        SeccompRule::new(vec![not_equal(2, 0)?]).map_err(|error| error.to_string())?,
        SeccompRule::new(invalid_pair_types).map_err(|error| error.to_string())?,
    ];
    let mut rules = BTreeMap::from([
        (libc::SYS_socket, vec![socket_rule]),
        (libc::SYS_socketpair, pair_rules),
        (libc::SYS_io_uring_setup, Vec::new()),
        (libc::SYS_io_uring_enter, Vec::new()),
        (libc::SYS_io_uring_register, Vec::new()),
    ]);
    #[cfg(target_arch = "x86_64")]
    for (syscall, syscall_rules) in rules.clone() {
        rules.insert(syscall | 0x4000_0000, syscall_rules);
    }
    SeccompFilter::new(
        rules,
        SeccompAction::Allow,
        SeccompAction::Errno(libc::EACCES as u32),
        std::env::consts::ARCH
            .try_into()
            .map_err(|error| format!("unsupported seccomp architecture: {error}"))?,
    )
    .map_err(|error| error.to_string())?
    .try_into()
    .map_err(|error: seccompiler::BackendError| error.to_string())
}

/// The hardening features which have actually been installed in a child.
///
/// `Restricted` is intentional. It means bwrap namespace/mount isolation,
/// descriptor closure, environment filtering, and a successful deny probe
/// are present, but V0's Landlock and custom seccomp requirements are not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxSecurityPosture {
    Restricted,
    KernelHardened,
}

/// Filesystem authority actually requested from the Linux launcher for the
/// workspace mount. This is independent from network isolation and from the
/// authorization ceiling: a caller must choose it explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxWorkspaceAccess {
    ReadOnly,
    ReadWrite,
}

/// Facts established by the real launcher preflight for a concrete command.
/// The receipt is attached to the prepared command and can be completed with
/// the child exit result by the execution adapter; it is never reconstructed
/// from a model-supplied input field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxLaunchReceipt {
    pub backend: &'static str,
    pub workspace_access: SandboxWorkspaceAccess,
    pub network_enabled: bool,
    pub kernel_hardening_applied: bool,
    pub protected_roots_denied: bool,
    pub inherited_fds_closed: bool,
    pub environment_allowlist_enforced: bool,
    pub prepared_at_ms: u64,
}

/// A concrete result of the Linux preflight, rather than a best-effort claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxPreflight {
    pub bwrap_path: PathBuf,
    /// 已通过内部协议验证、将被只读挂载进沙箱的同一个 Cowd 文件。
    pub cowd_binary_path: PathBuf,
    pub deny_probe_passed: bool,
    pub no_new_privs_verified: bool,
    pub inherited_fds_closed: bool,
    pub environment_allowlist_enforced: bool,
    pub protected_roots_denied: bool,
    pub posture: SandboxSecurityPosture,
    pub unavailable_hardening: Vec<String>,
}

impl SandboxPreflight {
    #[must_use]
    pub fn is_kernel_hardened(&self) -> bool {
        self.posture == SandboxSecurityPosture::KernelHardened
    }

    pub fn require_kernel_hardening(&self) -> Result<(), SandboxError> {
        if self.is_kernel_hardened() {
            Ok(())
        } else {
            Err(SandboxError::KernelHardeningUnavailable(
                self.unavailable_hardening.join("; "),
            ))
        }
    }
}

#[derive(Debug, Error)]
pub enum SandboxError {
    #[error("rootless sandbox launcher is unavailable: bubblewrap (`bwrap`) was not found")]
    LauncherUnavailable,
    #[error("rootless sandbox launcher is only supported on Linux")]
    UnsupportedPlatform,
    #[error("rootless sandbox launcher bootstrap shell `{0}` is unavailable")]
    BootstrapUnavailable(String),
    #[error("sandbox path `{0}` does not exist")]
    MissingPath(String),
    #[error("sandbox path `{0}` is not absolute")]
    RelativePath(String),
    #[error("protected control root `{protected_root}` would be exposed through `{visible_root}`")]
    ProtectedRootExposed {
        protected_root: String,
        visible_root: String,
    },
    #[error("environment key `{0}` is not allowed in an untrusted sandbox")]
    DisallowedEnvironment(String),
    #[error("environment key `{0}` is malformed")]
    MalformedEnvironment(String),
    #[error("sandbox probe failed: {0}")]
    ProbeFailed(String),
    #[error("sandbox kernel hardening is required but unavailable: {0}")]
    KernelHardeningUnavailable(String),
    #[error("bounded launcher preparation failed at {stage}: {source}")]
    BoundedPreparation { stage: &'static str, #[source] source: Box<SandboxError> },
}

impl SandboxError {
    fn bounded_preparation(stage: &'static str, source: SandboxError) -> Self {
        Self::BoundedPreparation { stage, source: Box::new(source) }
    }

    /// Fixed, non-sensitive evidence; original error text is never included.
    pub fn bounded_preparation_reason_code(&self) -> String {
        let (stage, source) = match self {
            Self::BoundedPreparation { stage, source } => (*stage, source.as_ref()),
            other => ("unclassified", other),
        };
        let (stage, source) = match source {
            Self::BoundedPreparation { stage: inner, source } if stage == "preflight" => (*inner, source.as_ref()),
            _ => (stage, source),
        };
        let stage = match stage {
            "profile" | "workspace_canonical" | "workspace_overlap" | "workspace_open"
            | "preflight" | "launcher_image" | "platform" | "preflight_spec" | "preflight_launcher"
            | "preflight_bootstrap" | "preflight_image" | "preflight_fixture" | "preflight_probe_spec"
            | "preflight_probe_args" | "preflight_probe_run" | "preflight_probe_result" | "preflight_hardening" => stage,
            _ => "unclassified",
        };
        let category = match source {
            Self::LauncherUnavailable => "launcher_unavailable",
            Self::UnsupportedPlatform => "unsupported_platform",
            Self::BootstrapUnavailable(_) => "bootstrap_unavailable",
            Self::MissingPath(_) => "missing_path",
            Self::RelativePath(_) => "relative_path",
            Self::ProtectedRootExposed { .. } => "protected_root",
            Self::DisallowedEnvironment(_) => "disallowed_environment",
            Self::MalformedEnvironment(_) => "malformed_environment",
            Self::ProbeFailed(_) => "probe_failed",
            Self::KernelHardeningUnavailable(_) => "hardening_unavailable",
            Self::BoundedPreparation { .. } => "nested_preparation",
        };
        format!("bounded_launcher_prepare_failed_{stage}_{category}")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxLaunchSpec {
    pub workspace_root: PathBuf,
    pub working_directory: Option<PathBuf>,
    pub readable_roots: Vec<PathBuf>,
    pub writable_roots: Vec<PathBuf>,
    /// Cowd 自有工具根。它们只读挂载，并只把约定的 bin 目录加入沙箱 PATH。
    /// 与普通 readable_roots 分开建模，避免任意可读目录影响命令解析。
    pub tool_roots: Vec<PathBuf>,
    /// Explicit control-plane paths which must never become visible to the
    /// child. The constructor also derives config and broker paths from the
    /// environment when available.
    pub protected_roots: Vec<PathBuf>,
    pub network_enabled: bool,
    /// Whether the workspace itself is writable. Additional writable roots
    /// remain explicit and never make a read-only workspace writable.
    pub workspace_access: SandboxWorkspaceAccess,
    /// Only a small display/locale/proxy allowlist is accepted. Credentials,
    /// config paths, dynamic-loader values, and `COWD_*` control variables are
    /// rejected.
    pub environment: Vec<(String, String)>,
    /// Require the verified Landlock+seccomp inner launcher. Untrusted callers
    /// use this terminal posture and fail closed when it cannot be established.
    pub require_kernel_hardening: bool,
}

impl SandboxLaunchSpec {
    #[must_use]
    pub fn workspace(workspace_root: impl Into<PathBuf>) -> Self {
        Self {
            workspace_root: workspace_root.into(),
            working_directory: None,
            readable_roots: Vec::new(),
            writable_roots: Vec::new(),
            tool_roots: Vec::new(),
            protected_roots: default_protected_roots(),
            network_enabled: true,
            workspace_access: SandboxWorkspaceAccess::ReadWrite,
            environment: Vec::new(),
            require_kernel_hardening: true,
        }
    }

    /// Add a protected config, broker, registry, database, or control path.
    /// It is never mounted and launch fails if another declared root would
    /// cover it.
    pub fn protect_root(&mut self, root: impl Into<PathBuf>) {
        self.protected_roots.push(root.into());
    }

    pub fn validate(&self) -> Result<(), SandboxError> {
        validate_root(&self.workspace_root)?;
        let workspace = canonical(&self.workspace_root)?;
        if let Some(working_directory) = &self.working_directory {
            validate_root(working_directory)?;
            let working_directory = canonical(working_directory)?;
            if !working_directory.starts_with(&workspace) {
                return Err(SandboxError::MissingPath(format!(
                    "working directory `{}` is outside workspace `{}`",
                    working_directory.display(),
                    workspace.display()
                )));
            }
        }

        let visible_roots = self.visible_roots(&workspace)?;
        let tool_roots = self
            .tool_roots
            .iter()
            .map(|root| {
                let root = canonical(root)?;
                if root.file_name().and_then(|name| name.to_str()) != Some("tools") {
                    return Err(SandboxError::MissingPath(format!(
                        "sandbox tool root `{}` must end with /tools",
                        root.display()
                    )));
                }
                Ok(root)
            })
            .collect::<Result<Vec<_>, _>>()?;
        for protected_root in self
            .protected_roots
            .iter()
            .map(|root| normalize_absolute(root))
        {
            let protected_root = protected_root?;
            for visible_root in &visible_roots {
                // 只允许显式 tool root 作为受保护 Cowd 配置根的只读子树。
                // Bubblewrap 仅绑定该子树，兄弟目录（credentials/storage 等）仍不可见。
                if tool_roots.iter().any(|tool_root| tool_root == visible_root)
                    && visible_root == &protected_root.join("tools")
                {
                    continue;
                }
                if paths_overlap(&protected_root, visible_root) {
                    return Err(SandboxError::ProtectedRootExposed {
                        protected_root: protected_root.display().to_string(),
                        visible_root: visible_root.display().to_string(),
                    });
                }
            }
        }
        validate_environment(&self.environment)
    }

    fn visible_roots(&self, workspace: &Path) -> Result<Vec<PathBuf>, SandboxError> {
        let mut roots = vec![workspace.to_path_buf()];
        for root in self
            .readable_roots
            .iter()
            .chain(&self.writable_roots)
            .chain(&self.tool_roots)
        {
            validate_root(root)?;
            roots.push(canonical(root)?);
        }
        roots.extend(
            SYSTEM_READ_ONLY_ROOTS
                .iter()
                .map(PathBuf::from)
                .filter(|root| root.exists()),
        );
        Ok(roots)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedSandboxCommand {
    /// A minimal trusted host wrapper. It only closes descriptors then `exec`s
    /// the absolute bwrap binary; untrusted shell content remains after the
    /// bwrap `--` boundary.
    host_args: Vec<String>,
    host_project: PathBuf,
    pub program: String,
    pub args: Vec<String>,
    /// Empty by design. Bubblewrap receives its allowlisted environment via
    /// `--clearenv`/`--setenv`; the wrapper itself starts with `env_clear`.
    pub environment: Vec<(String, String)>,
    pub preflight: SandboxPreflight,
    pub launch_receipt: SandboxLaunchReceipt,
}

impl PreparedSandboxCommand {
    #[must_use]
    pub fn into_command(&self) -> Command {
        let mut command = Command::new(&self.program);
        command.args(&self.args);
        command.env_clear();
        command.envs(self.environment.iter().cloned());
        command
    }

    #[must_use]
    pub fn security_posture(&self) -> SandboxSecurityPosture {
        self.preflight.posture
    }

    #[must_use]
    pub fn launch_receipt(&self) -> &SandboxLaunchReceipt {
        &self.launch_receipt
    }
}

/// Check that the host can start the restricted bwrap sandbox. This function
/// never falls back to a naked host command.
pub fn probe() -> Result<(), SandboxError> {
    let fixture = ProbeFixture::new()?;
    let mut spec = SandboxLaunchSpec::workspace(&fixture.workspace);
    spec.protect_root(&fixture.control);
    preflight(&spec).map(|_| ())
}

/// Perform the fail-closed launch preflight for a concrete launch request.
///
/// The probe starts a real bwrap child and checks that an unbound control file
/// is not observable, `no_new_privs` is active, and the allowlisted
/// environment is the only child environment. If it cannot establish those
/// facts, command construction fails.
pub fn preflight(spec: &SandboxLaunchSpec) -> Result<SandboxPreflight, SandboxError> {
    preflight_with_stages(spec, false)
}

fn preflight_with_stages(spec: &SandboxLaunchSpec, bounded: bool) -> Result<SandboxPreflight, SandboxError> {
    let tag = |stage, error| if bounded { SandboxError::bounded_preparation(stage, error) } else { error };
    if !cfg!(target_os = "linux") {
        return Err(tag("platform", SandboxError::UnsupportedPlatform));
    }
    spec.validate().map_err(|error| tag("preflight_spec", error))?;
    let bwrap_path = bwrap_path().ok_or_else(|| tag("preflight_launcher", SandboxError::LauncherUnavailable))?;
    ensure_bootstrap_shell().map_err(|error| tag("preflight_bootstrap", error))?;
    let cowd_binary_path = launcher_binary_path().map_err(|error| tag("preflight_image", error))?;
    let fixture = ProbeFixture::new().map_err(|error| tag("preflight_fixture", error))?;
    let mut probe_spec = SandboxLaunchSpec::workspace(&spec.workspace_root);
    probe_spec.working_directory = spec.working_directory.clone();
    probe_spec.readable_roots = spec.readable_roots.clone();
    probe_spec.writable_roots = spec.writable_roots.clone();
    probe_spec.protected_roots = spec.protected_roots.clone();
    probe_spec.protect_root(&fixture.control);
    probe_spec.network_enabled = spec.network_enabled;
    probe_spec.workspace_access = spec.workspace_access;
    probe_spec.environment = spec.environment.clone();
    probe_spec.require_kernel_hardening = spec.require_kernel_hardening;
    probe_spec.validate().map_err(|error| tag("preflight_probe_spec", error))?;

    let args = bwrap_args(
        &bwrap_path,
        &cowd_binary_path,
        &probe_spec,
        probe_command(&fixture.control, &probe_spec.environment),
    ).map_err(|error| tag("preflight_probe_args", error))?;
    let output = run_fd_closing_wrapper(&bwrap_path, &cowd_binary_path, &args).map_err(|error| tag("preflight_probe_run", error))?;
    if !output.status.success() {
        return Err(tag("preflight_probe_result", SandboxError::ProbeFailed(render_output(&output))));
    }

    let preflight = SandboxPreflight {
        bwrap_path,
        cowd_binary_path,
        deny_probe_passed: true,
        no_new_privs_verified: true,
        inherited_fds_closed: true,
        environment_allowlist_enforced: true,
        protected_roots_denied: true,
        posture: SandboxSecurityPosture::KernelHardened,
        unavailable_hardening: Vec::new(),
    };
    if spec.require_kernel_hardening {
        preflight.require_kernel_hardening().map_err(|error| tag("preflight_hardening", error))?;
    }
    Ok(preflight)
}

pub fn shell_command(
    command: &str,
    spec: &SandboxLaunchSpec,
) -> Result<PreparedSandboxCommand, SandboxError> {
    let preflight = preflight(spec)?;
    let args = bwrap_args(
        &preflight.bwrap_path,
        &preflight.cowd_binary_path,
        spec,
        command.to_string(),
    )?;
    Ok(PreparedSandboxCommand {
        host_args: args.clone(),
        host_project: canonical(&spec.workspace_root)?,
        program: BOOTSTRAP_SHELL.to_string(),
        args: vec![
            "-c".to_string(),
            fd_closing_script(&preflight.bwrap_path, &preflight.cowd_binary_path, &args),
        ],
        environment: Vec::new(),
        launch_receipt: SandboxLaunchReceipt {
            backend: "bubblewrap+landlock+seccomp",
            workspace_access: spec.workspace_access,
            network_enabled: spec.network_enabled,
            kernel_hardening_applied: preflight.is_kernel_hardened(),
            protected_roots_denied: preflight.protected_roots_denied,
            inherited_fds_closed: preflight.inherited_fds_closed,
            environment_allowlist_enforced: preflight.environment_allowlist_enforced,
            prepared_at_ms: now_ms(),
        },
        preflight,
    })
}

/// Launch an executable contained in the declared workspace without exposing
/// the parent Gateway filesystem to the child.
pub fn program_command(
    program: &Path,
    spec: &SandboxLaunchSpec,
) -> Result<PreparedSandboxCommand, SandboxError> {
    program_command_with_args(program, &[], spec)
}

/// Build a hardened command for an executable and its exact argv.
///
/// Arguments must be placed inside the inner command before Bubblewrap is
/// assembled. Appending them to [`PreparedSandboxCommand::args`] would append
/// to the outer launcher protocol instead of the managed program.
pub fn program_command_with_args(
    program: &Path,
    args: &[String],
    spec: &SandboxLaunchSpec,
) -> Result<PreparedSandboxCommand, SandboxError> {
    let workspace = canonical(&spec.workspace_root)?;
    let program = canonical(program)?;
    if !program.starts_with(&workspace) {
        return Err(SandboxError::MissingPath(format!(
            "program `{}` is outside workspace `{}`",
            program.display(),
            workspace.display()
        )));
    }
    let mut command = format!("exec {}", shell_quote(&program.display().to_string()));
    for arg in args {
        command.push(' ');
        command.push_str(&shell_quote(arg));
    }
    shell_command(&command, spec)
}

fn bwrap_args(
    _bwrap_path: &Path,
    _inner_launcher: &Path,
    spec: &SandboxLaunchSpec,
    command: String,
) -> Result<Vec<String>, SandboxError> {
    spec.validate()?;
    let workspace = canonical(&spec.workspace_root)?;
    let working_directory = spec
        .working_directory
        .as_deref()
        .map(canonical)
        .transpose()?
        .unwrap_or_else(|| workspace.clone());
    let mut args = vec![
        "--die-with-parent".to_string(),
        "--new-session".to_string(),
        "--unshare-user".to_string(),
        "--uid".to_string(),
        "0".to_string(),
        "--gid".to_string(),
        "0".to_string(),
        "--unshare-pid".to_string(),
        "--unshare-ipc".to_string(),
        "--unshare-uts".to_string(),
        "--unshare-cgroup".to_string(),
        "--disable-userns".to_string(),
        "--assert-userns-disabled".to_string(),
    ];
    if !spec.network_enabled {
        args.push("--unshare-net".to_string());
    }

    args.push("--clearenv".to_string());
    append_environment(&mut args, &spec.environment, &spec.tool_roots)?;
    for root in SYSTEM_READ_ONLY_ROOTS {
        if Path::new(root).exists() {
            args.extend([
                "--ro-bind".to_string(),
                (*root).to_string(),
                (*root).to_string(),
            ]);
        }
    }
    args.extend([
        "--proc".to_string(),
        "/proc".to_string(),
        "--dev".to_string(),
        "/dev".to_string(),
        "--tmpfs".to_string(),
        "/tmp".to_string(),
        "--dir".to_string(),
        "/home".to_string(),
        "--dir".to_string(),
        "/run".to_string(),
        "--ro-bind".to_string(),
        "/proc/self/fd/3".to_string(),
        INNER_LAUNCHER_PATH.to_string(),
    ]);
    append_network_resolver_bind(&mut args, spec.network_enabled)?;

    let writable = spec
        .writable_roots
        .iter()
        .map(|root| canonical(root))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let mut readable = spec
        .readable_roots
        .iter()
        .chain(&spec.tool_roots)
        .map(|root| canonical(root))
        .collect::<Result<BTreeSet<_>, _>>()?;
    readable.remove(&workspace);
    for root in &writable {
        readable.remove(root);
    }

    bind_root(
        &mut args,
        &workspace,
        spec.workspace_access == SandboxWorkspaceAccess::ReadWrite,
    );
    for root in readable {
        bind_root(&mut args, &root, false);
    }
    for root in writable {
        bind_root(&mut args, &root, true);
    }
    args.extend([
        "--chdir".to_string(),
        working_directory.display().to_string(),
        "--".to_string(),
        BOOTSTRAP_SHELL.to_string(),
        "-c".to_string(),
        "exec 3>&-; exec /usr/bin/env -u SHLVL -u _ \"$@\"".to_string(),
        "cowd-sandbox-bootstrap".to_string(),
        INNER_LAUNCHER_PATH.to_string(),
        INTERNAL_DISPATCH.to_string(),
        INTERNAL_ROLE.to_string(),
        "--inner".to_string(),
        "--workspace".to_string(),
        workspace.display().to_string(),
    ]);
    if spec.workspace_access == SandboxWorkspaceAccess::ReadOnly {
        args.push("--workspace-read-only".to_string());
    }
    if !spec.network_enabled {
        args.push("--network-isolated".to_string());
    }
    for root in &spec.writable_roots {
        args.push("--writable".to_string());
        args.push(canonical(root)?.display().to_string());
    }
    args.extend(["--".to_string(), command]);
    Ok(args)
}

fn append_network_resolver_bind(
    args: &mut Vec<String>,
    network_enabled: bool,
) -> Result<(), SandboxError> {
    if !network_enabled {
        return Ok(());
    }
    let resolver = canonical(Path::new("/etc/resolv.conf"))?;
    if resolver.starts_with("/etc") {
        return Ok(());
    }
    let allowed = [
        Path::new("/run/systemd/resolve"),
        Path::new("/run/NetworkManager"),
        Path::new("/run/resolvconf"),
    ];
    if !allowed.iter().any(|root| resolver.starts_with(root)) {
        return Err(SandboxError::MissingPath(format!(
            "resolver target `{}` is outside the supported runtime roots",
            resolver.display()
        )));
    }
    let parent = resolver.parent().ok_or_else(|| {
        SandboxError::MissingPath("resolver target has no parent directory".to_string())
    })?;
    let relative = parent
        .strip_prefix("/run")
        .map_err(|_| SandboxError::MissingPath("resolver target is not below /run".to_string()))?;
    let mut target_parent = PathBuf::from("/run");
    for component in relative.components() {
        target_parent.push(component.as_os_str());
        args.extend(["--dir".to_string(), target_parent.display().to_string()]);
    }
    args.extend([
        "--ro-bind".to_string(),
        resolver.display().to_string(),
        resolver.display().to_string(),
    ]);
    Ok(())
}

fn launcher_binary_path() -> Result<PathBuf, SandboxError> {
    if COWD_PROCESS_HOST.load(Ordering::Acquire) {
        return process_host_executable_path();
    }
    // P7: tests and CI run under the libtest harness, which cannot carry the
    // inner role itself. An explicit, protocol-verified launcher binary
    // (e.g. `target/debug/cowd`) makes the real bwrap sandbox tests
    // executable without weakening production (the variable is unset there).
    if let Some(explicit) = env::var_os("COWD_SANDBOX_LAUNCHER_BINARY") {
        let explicit = PathBuf::from(explicit);
        if explicit.is_file() {
            let candidate = canonical(&explicit)?;
            if launcher_protocol_matches(&candidate) {
                return Ok(candidate);
            }
            return Err(SandboxError::KernelHardeningUnavailable(
                "COWD_SANDBOX_LAUNCHER_BINARY does not expose the sandbox inner process role"
                    .to_string(),
            ));
        }
    }
    let current = env::current_exe().map_err(|error| {
        SandboxError::ProbeFailed(format!("resolve current executable: {error}"))
    })?;
    let parent = current.parent().ok_or_else(|| {
        SandboxError::ProbeFailed("current executable has no parent directory".to_string())
    })?;
    let mut candidates = Vec::new();
    candidates.push(current.clone());
    candidates.push(parent.join("cowd"));
    if let Some(grandparent) = parent.parent() {
        candidates.push(grandparent.join("cowd"));
    }
    let mut inspected = HashSet::new();
    for candidate in candidates {
        if !candidate.is_file() {
            continue;
        }
        let candidate = canonical(&candidate)?;
        if inspected.insert(candidate.clone()) && launcher_protocol_matches(&candidate) {
            return Ok(candidate);
        }
    }
    Err(SandboxError::KernelHardeningUnavailable(
        "the current Cowd executable does not expose the sandbox inner process role".to_string(),
    ))
}

#[cfg(target_os = "linux")]
fn process_host_executable_path() -> Result<PathBuf, SandboxError> {
    let path = PathBuf::from(format!("/proc/{}/exe", std::process::id()));
    fs::metadata(&path).map_err(|error| {
        SandboxError::ProbeFailed(format!("pin running Cowd executable image: {error}"))
    })?;
    Ok(path)
}

#[cfg(not(target_os = "linux"))]
fn process_host_executable_path() -> Result<PathBuf, SandboxError> {
    Err(SandboxError::UnsupportedPlatform)
}

fn launcher_protocol_matches(path: &Path) -> bool {
    // The launcher binary can be rewritten while the acceptance lanes run (a
    // fresh build replaces it). Executing a binary that is being written
    // fails with ETXTBSY, which must not be mistaken for "this binary does
    // not expose the inner role": that turns a build into a false acceptance
    // failure.
    let mut output = None;
    for attempt in 0..5 {
        let attempt_output = Command::new(path)
            .args([INTERNAL_DISPATCH, INTERNAL_ROLE, "--protocol-version"])
            .env_clear()
            .stdin(Stdio::null())
            .output();
        match attempt_output {
            Ok(result) => {
                output = Some(result);
                break;
            }
            Err(error) if error.raw_os_error() == Some(libc::ETXTBSY) && attempt < 4 => {
                std::thread::sleep(std::time::Duration::from_millis(200));
            }
            Err(_) => break,
        }
    }
    let Some(output) = output else {
        return false;
    };
    let actual = String::from_utf8_lossy(&output.stdout).trim().to_string();
    output.status.success() && actual == LAUNCHER_PROTOCOL
}

fn append_environment(
    args: &mut Vec<String>,
    environment: &[(String, String)],
    tool_roots: &[PathBuf],
) -> Result<(), SandboxError> {
    validate_environment(environment)?;
    let mut path_entries = Vec::new();
    for root in tool_roots {
        let root = canonical(root)?;
        for candidate in [root.join("bin"), root.join("node_modules").join(".bin")] {
            if candidate.is_dir() {
                path_entries.push(candidate);
            }
        }
    }
    path_entries.push(PathBuf::from(SANDBOX_PATH));
    let sandbox_path = path_entries
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(":");
    for (key, value) in [
        ("HOME", "/home/cowd"),
        ("TMPDIR", "/tmp"),
        ("PATH", sandbox_path.as_str()),
        ("COWD_SANDBOX", "rootless-kernel-hardened"),
    ] {
        args.extend(["--setenv".to_string(), key.to_string(), value.to_string()]);
    }
    for (key, value) in environment {
        args.extend(["--setenv".to_string(), key.clone(), value.clone()]);
    }
    Ok(())
}

fn run_fd_closing_wrapper(
    bwrap_path: &Path,
    inner_launcher: &Path,
    args: &[String],
) -> Result<Output, SandboxError> {
    ensure_bootstrap_shell()?;
    let image = fs::File::open(inner_launcher)
        .map_err(|error| SandboxError::ProbeFailed(error.to_string()))?;
    let script = format!(
        "exec 3</proc/self/fd/0 || exit 125; exec 0</dev/null; {}",
        fd_closing_script(bwrap_path, Path::new("/proc/self/fd/3"), args)
    );
    Command::new(BOOTSTRAP_SHELL)
        .arg("-c")
        .arg(script)
        .env_clear()
        .stdin(Stdio::from(image))
        .output()
        .map_err(|error| SandboxError::ProbeFailed(error.to_string()))
}

fn fd_closing_script(bwrap_path: &Path, inner_launcher: &Path, args: &[String]) -> String {
    let bwrap = shell_quote(&bwrap_path.display().to_string());
    let inner_launcher = shell_quote(&inner_launcher.display().to_string());
    let args = args
        .iter()
        .map(|argument| shell_quote(argument))
        .collect::<Vec<_>>()
        .join(" ");
    // This shell is trusted bootstrap only: all untrusted command text is a
    // single shell-quoted bwrap argument after `--`. Rust's safe process API
    // has no close_range primitive, so open exactly one trusted Cowd image
    // descriptor, enumerate this bootstrap process's Linux FD table, and
    // close every other descriptor above stderr before exec. bwrap resolves
    // its own `/proc/self/fd/3`, preserving the running Cowd inode across an
    // atomic on-disk replacement. Absence of procfs is a hard failure.
    format!(
        "exec 3<{inner_launcher} || {{ echo 'sandbox bootstrap cannot pin the running Cowd image' >&2; exit 125; }}; [ -d /proc/$$/fd ] || {{ echo 'sandbox bootstrap requires /proc/self/fd' >&2; exit 125; }}; for entry in /proc/$$/fd/*; do fd=${{entry##*/}}; case \"$fd\" in 0|1|2|3|*[!0-9]*|'') ;; *) eval \"exec $fd>&-\" ;; esac; done; exec {bwrap} {args}"
    )
}

fn probe_command(control_path: &Path, environment: &[(String, String)]) -> String {
    let control = shell_quote(&control_path.display().to_string());
    let custom_keys = environment
        .iter()
        .map(|(key, _)| key.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "set -eu; test ! -e {control}; test ! -r {control}; test \"${{HOME}}\" = /home/cowd; test \"${{COWD_SANDBOX}}\" = rootless-kernel-hardened; test -z \"${{COWD_CONFIG_HOME+x}}\"; allowed=' HOME TMPDIR PATH COWD_SANDBOX PWD {custom_keys} '; /usr/bin/env | while IFS= read -r entry; do key=${{entry%%=*}}; case \"$allowed\" in *\" $key \"*) ;; *) exit 126 ;; esac; done; found=0; while IFS=: read -r key value; do if [ \"$key\" = NoNewPrivs ]; then test \"$value\" -eq 1; found=1; fi; done < /proc/self/status; test \"$found\" -eq 1"
    )
}

fn render_output(output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    match (stdout.is_empty(), stderr.is_empty()) {
        (true, true) => format!("exit status {}", output.status),
        (false, true) => format!("exit status {}; stdout: {stdout}", output.status),
        (true, false) => format!("exit status {}; stderr: {stderr}", output.status),
        (false, false) => format!(
            "exit status {}; stdout: {stdout}; stderr: {stderr}",
            output.status
        ),
    }
}

fn default_protected_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    for key in [
        "COWD_CONFIG_HOME",
        "COWD_AUTH_BROKER_ROOT",
        "COWD_AUTH_BROKER_SOCKET",
        "COWD_CONTROL_ROOT",
        "COWD_RUNTIME_EVENTS_PATH",
    ] {
        if let Some(path) = env::var_os(key).map(PathBuf::from) {
            if path.is_absolute() {
                roots.push(path);
            }
        }
    }
    if !roots.iter().any(|root| root.ends_with(".cowd")) {
        if let Some(home) = env::var_os("HOME") {
            roots.push(PathBuf::from(home).join(".cowd"));
        }
    }
    roots
}

fn validate_environment(environment: &[(String, String)]) -> Result<(), SandboxError> {
    let mut seen = HashSet::new();
    for (key, value) in environment {
        if !valid_environment_key(key) {
            return Err(SandboxError::MalformedEnvironment(key.clone()));
        }
        // The pair list is the *entire* child environment (`--clearenv` then
        // `--setenv`). Any explicitly listed key is therefore an intentional
        // allowlist entry from the operator/tool policy; the remaining hard
        // constraints are syntax, uniqueness, control-plane isolation, and
        // NUL safety. Sensitive host variables must be filtered *before*
        // this point by the shell environment policy (T5), never smuggled
        // through the generic locale/proxy allowlist.
        if key.starts_with("COWD_") || !seen.insert(key) || value.contains('\0') {
            return Err(SandboxError::DisallowedEnvironment(key.clone()));
        }
    }
    Ok(())
}

fn valid_environment_key(key: &str) -> bool {
    key.chars().enumerate().all(|(index, character)| {
        matches!(
            (index, character),
            (0, 'A'..='Z' | 'a'..='z' | '_')
                | (_, 'A'..='Z' | 'a'..='z' | '0'..='9' | '_')
        )
    })
}

fn bwrap_path() -> Option<PathBuf> {
    let paths = env::var_os("PATH")?;
    env::split_paths(&paths)
        .map(|path| path.join("bwrap"))
        .find(|candidate| candidate.is_file())
        .and_then(|candidate| candidate.canonicalize().ok())
}

fn ensure_bootstrap_shell() -> Result<(), SandboxError> {
    if Path::new(BOOTSTRAP_SHELL).is_file() {
        Ok(())
    } else {
        Err(SandboxError::BootstrapUnavailable(
            BOOTSTRAP_SHELL.to_string(),
        ))
    }
}

fn canonical(path: &Path) -> Result<PathBuf, SandboxError> {
    path.canonicalize()
        .map_err(|_| SandboxError::MissingPath(path.display().to_string()))
}

fn normalize_absolute(path: &Path) -> Result<PathBuf, SandboxError> {
    if !path.is_absolute() {
        return Err(SandboxError::RelativePath(path.display().to_string()));
    }
    if path.exists() {
        return canonical(path);
    }
    let mut normalized = PathBuf::from("/");
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(value) => normalized.push(value),
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Prefix(_) => {
                return Err(SandboxError::RelativePath(path.display().to_string()));
            }
        }
    }
    Ok(normalized)
}

fn validate_root(path: &Path) -> Result<(), SandboxError> {
    if !path.is_absolute() {
        return Err(SandboxError::RelativePath(path.display().to_string()));
    }
    if !path.exists() {
        return Err(SandboxError::MissingPath(path.display().to_string()));
    }
    Ok(())
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn bind_root(args: &mut Vec<String>, root: &Path, writable: bool) {
    append_parent_dirs(args, root);
    args.push(if writable { "--bind" } else { "--ro-bind" }.to_string());
    args.push(root.display().to_string());
    args.push(root.display().to_string());
}

fn append_parent_dirs(args: &mut Vec<String>, path: &Path) {
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir => current.push("/"),
            Component::Normal(part) => {
                current.push(part);
                let value = current.display().to_string();
                if !SYSTEM_READ_ONLY_ROOTS.contains(&value.as_str()) {
                    args.extend(["--dir".to_string(), value]);
                }
            }
            Component::CurDir | Component::ParentDir | Component::Prefix(_) => {}
        }
    }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

struct ProbeFixture {
    root: PathBuf,
    workspace: PathBuf,
    control: PathBuf,
}

impl ProbeFixture {
    fn new() -> Result<Self, SandboxError> {
        let root = create_probe_root()?;
        fs::create_dir(root.join("workspace"))
            .map_err(|error| SandboxError::ProbeFailed(error.to_string()))?;
        let control = root.join("control-plane-secret");
        fs::write(&control, "must-not-be-visible")
            .map_err(|error| SandboxError::ProbeFailed(error.to_string()))?;
        Ok(Self {
            workspace: root.join("workspace"),
            control,
            root,
        })
    }
}

fn create_probe_root() -> Result<PathBuf, SandboxError> {
    for _ in 0..16 {
        let mut nonce = [0_u8; 16];
        fs::File::open("/dev/urandom")
            .and_then(|mut source| source.read_exact(&mut nonce))
            .map_err(|error| SandboxError::ProbeFailed(error.to_string()))?;
        let suffix = nonce
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let root = env::temp_dir().join(format!("cowd-sandbox-probe-{suffix}"));
        match fs::create_dir(&root) {
            Ok(()) => return Ok(root),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(SandboxError::ProbeFailed(error.to_string())),
        }
    }
    Err(SandboxError::ProbeFailed(
        "could not allocate an exclusive probe directory".to_string(),
    ))
}

impl Drop for ProbeFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[cfg(target_os = "linux")]
mod bounded {
    use super::*;
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Write};
    use std::os::fd::{AsFd, AsRawFd, OwnedFd};
    use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt};
    use std::os::unix::process::CommandExt;
    use std::os::unix::net::UnixDatagram;
    use std::time::{Duration, Instant};

    const FRAME_BYTES: usize = 128;
    const MAGIC: &[u8; 8] = b"CWDBP279";
    const CAPTURE: usize = 2 * 1024 * 1024;
    pub struct PreparedBoundedCommand {
        state: PreparedBoundedState,
    }
    struct PreparedBoundedState {
        spec: SandboxLaunchSpec,
        command: String,
        deadline: Instant,
        digest: [u8; 32],
        preflight: SandboxPreflight,
        project: File,
        image: File,
    }
    pub struct BoundedSandboxObservation {
        state: BoundedObservationState,
    }
    struct BoundedObservationState {
        started: bool,
        exit: Option<i32>,
        signal: Option<i32>,
        reason: String,
        stop: &'static str,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        complete: bool,
        identity: Option<(u64, u32, u64)>,
        children_reaped: bool,
        init_reaped: bool,
        protected: bool,
        profile: Option<SandboxLaunchReceipt>,
    }
    impl BoundedSandboxObservation {
        pub fn started(&self) -> bool {
            self.state.started
        }
        pub fn exit_code(&self) -> Option<i32> {
            self.state.exit
        }
        pub fn signal(&self) -> Option<i32> {
            self.state.signal
        }
        pub fn reason_code(&self) -> &str {
            &self.state.reason
        }
        pub fn stop_reason(&self) -> &str {
            self.state.stop
        }
        pub fn stdout(&self) -> &[u8] {
            &self.state.stdout
        }
        pub fn stderr(&self) -> &[u8] {
            &self.state.stderr
        }
        pub fn capture_complete(&self) -> bool {
            self.state.complete
        }
        pub fn identity(&self) -> Option<(u64, u32, u64)> {
            self.state.identity
        }
        pub fn all_children_reaped(&self) -> bool {
            self.state.children_reaped
        }
        pub fn init_reaped(&self) -> bool {
            self.state.init_reaped
        }
        pub fn status_channel_protected(&self) -> bool {
            self.state.protected
        }
        pub fn applied_profile(&self) -> Option<&SandboxLaunchReceipt> {
            self.state.profile.as_ref()
        }
    }
    fn empty(reason: &str) -> BoundedSandboxObservation {
        BoundedSandboxObservation {
            state: BoundedObservationState {
                started: false,
                exit: None,
                signal: None,
                reason: reason.into(),
                stop: "command_exit",
                stdout: Vec::new(),
                stderr: Vec::new(),
                complete: false,
                identity: None,
                children_reaped: false,
                init_reaped: false,
                protected: false,
                profile: None,
            },
        }
    }
    pub fn prepare_bounded_command(
        mut spec: SandboxLaunchSpec,
        command: String,
        timeout: Duration,
        admission_sha256: [u8; 32],
    ) -> Result<PreparedBoundedCommand, SandboxError> {
        let deadline = Instant::now() + timeout.min(Duration::from_secs(120));
        if spec.network_enabled
            || spec.workspace_access != SandboxWorkspaceAccess::ReadOnly
            || !spec.require_kernel_hardening
            || !spec.readable_roots.is_empty()
            || !spec.writable_roots.is_empty()
            || !spec.tool_roots.is_empty()
            || !spec.environment.is_empty()
            || command.len() > 131072
            || timeout.is_zero()
            || timeout > Duration::from_secs(120)
        {
            return Err(SandboxError::bounded_preparation("profile", SandboxError::ProbeFailed("bounded_profile_invalid".into())));
        }
        let root = canonical(&spec.workspace_root).map_err(|error| SandboxError::bounded_preparation("workspace_canonical", error))?;
        for system in SYSTEM_READ_ONLY_ROOTS {
            for runtime in [
                PathBuf::from(system),
                Path::new(system)
                    .canonicalize()
                    .unwrap_or_else(|_| PathBuf::from(system)),
            ] {
                if root.starts_with(&runtime) || runtime.starts_with(&root) {
                    return Err(SandboxError::bounded_preparation("workspace_overlap", SandboxError::ProbeFailed(
                        "bounded_system_root_overlap".into(),
                    )));
                }
            }
        }
        spec.working_directory = Some(root.clone());
        let project = rustix::fs::openat2(
            rustix::fs::CWD,
            root.as_path(),
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
            rustix::fs::ResolveFlags::NO_SYMLINKS | rustix::fs::ResolveFlags::NO_MAGICLINKS,
        )
        .map_err(|error| SandboxError::bounded_preparation("workspace_open", SandboxError::ProbeFailed(error.to_string())))?;
        let preflight = preflight_with_stages(&spec, true).map_err(|error| SandboxError::bounded_preparation("preflight", error))?;
        let image = File::open(&preflight.cowd_binary_path)
            .map_err(|error| SandboxError::bounded_preparation("launcher_image", SandboxError::ProbeFailed(error.to_string())))?;
        Ok(PreparedBoundedCommand {
            state: PreparedBoundedState {
                spec,
                command,
                deadline,
                digest: admission_sha256,
                preflight,
                project: File::from(project),
                image,
            },
        })
    }
    fn send_capabilities<'a>(
        socket: &UnixDatagram,
        descriptors: impl AsRef<[std::os::fd::BorrowedFd<'a>]>,
    ) -> Result<(), String> {
        let mut space = [std::mem::MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(5))];
        let mut ancillary = rustix::net::SendAncillaryBuffer::new(&mut space);
        if !ancillary.push(rustix::net::SendAncillaryMessage::ScmRights(
            descriptors.as_ref(),
        )) {
            return Err("bounded_handoff_capacity".into());
        }
        let sent = rustix::net::sendmsg(
            socket,
            &[std::io::IoSlice::new(b"CWDFD702")],
            &mut ancillary,
            rustix::net::SendFlags::NOSIGNAL | rustix::net::SendFlags::DONTWAIT,
        )
        .map_err(|error| error.to_string())?;
        if sent != 8 {
            return Err("bounded_handoff_incomplete".into());
        }
        Ok(())
    }
    fn receive_capabilities(socket: impl AsFd) -> Result<Vec<OwnedFd>, String> {
        receive_host_capabilities(socket, false)
    }
    fn receive_host_capabilities(
        socket: impl AsFd,
        streaming: bool,
    ) -> Result<Vec<OwnedFd>, String> {
        let mut space = [std::mem::MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(5))];
        let mut ancillary = rustix::net::RecvAncillaryBuffer::new(&mut space);
        let mut magic = [0; 8];
        let received = rustix::net::recvmsg(
            socket,
            &mut [std::io::IoSliceMut::new(&mut magic)],
            &mut ancillary,
            rustix::net::RecvFlags::CMSG_CLOEXEC | rustix::net::RecvFlags::DONTWAIT,
        )
        .map_err(|error| error.to_string())?;
        let mut descriptors = Vec::new();
        for message in ancillary.drain() {
            if let rustix::net::RecvAncillaryMessage::ScmRights(rights) = message {
                descriptors.extend(rights);
            }
        }
        if received.bytes != 8
            || magic != *b"CWDFD702"
            || received
                .flags
                .intersects(rustix::net::ReturnFlags::CTRUNC | rustix::net::ReturnFlags::TRUNC)
            || descriptors.len() != if streaming { 5 } else { 4 }
        {
            return Err("bounded_handoff_invalid".into());
        }
        let metadata = descriptors
            .iter()
            .map(|descriptor| rustix::fs::fstat(descriptor).map_err(|error| error.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        if rustix::fs::FileType::from_raw_mode(metadata[0].st_mode)
            != rustix::fs::FileType::Directory
            || rustix::fs::FileType::from_raw_mode(metadata[1].st_mode)
                != rustix::fs::FileType::Fifo
            || rustix::fs::FileType::from_raw_mode(metadata[2].st_mode)
                != rustix::fs::FileType::Fifo
            || metadata[1].st_ino == metadata[2].st_ino
            || rustix::fs::FileType::from_raw_mode(metadata[3].st_mode)
                != rustix::fs::FileType::RegularFile
            || (streaming
                && rustix::fs::FileType::from_raw_mode(metadata[4].st_mode)
                    != rustix::fs::FileType::Fifo)
        {
            return Err("bounded_handoff_types_invalid".into());
        }
        for (index, descriptor) in descriptors.iter().enumerate() {
            let access = rustix::fs::fcntl_getfl(descriptor).map_err(|error| error.to_string())?
                & rustix::fs::OFlags::ACCMODE;
            let expected = if index == 2 {
                rustix::fs::OFlags::WRONLY
            } else {
                rustix::fs::OFlags::RDONLY
            };
            if access != expected {
                return Err("bounded_handoff_access_invalid".into());
            }
        }
        Ok(descriptors)
    }
    pub(super) fn outer_entry(args: &[String]) -> ExitCode {
        match run_outer(args, false) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("bounded outer: {error}");
                ExitCode::from(125)
            }
        }
    }
    pub(super) fn stream_outer_entry(args: &[String]) -> ExitCode {
        match run_outer(args, true) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("stream outer: {error}");
                ExitCode::from(125)
            }
        }
    }
    fn run_outer(args: &[String], streaming: bool) -> Result<(), String> {
        if args.len() < 2
            || !Path::new(&args[0]).is_absolute()
            || Path::new(&args[0])
                .file_name()
                .is_none_or(|name| name != "bwrap")
            || !Path::new(&args[0]).is_file()
        {
            return Err("bounded_outer_arguments_invalid".into());
        }
        let descriptors = receive_host_capabilities(std::io::stdin(), streaming)?;
        let pinned = descriptors
            .iter()
            .map(|descriptor| {
                rustix::io::fcntl_dupfd_cloexec(descriptor, 16).map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        drop(descriptors);
        for descriptor in &pinned {
            rustix::io::fcntl_setfd(descriptor, rustix::io::FdFlags::empty())
                .map_err(|error| error.to_string())?;
        }
        let business_input = if streaming {
            format!(
                "exec 0</proc/self/fd/{} || exit 125;",
                pinned[4].as_raw_fd()
            )
        } else {
            "exec 0</dev/null;".into()
        };
        let script = format!(
            "exec 3</proc/self/fd/{} || exit 125; exec 4</proc/self/fd/{} || exit 125; exec 5>/proc/self/fd/{} || exit 125; exec 6</proc/self/fd/{} || exit 125; {business_input} for entry in /proc/$$/fd/*; do fd=${{entry##*/}}; case \"$fd\" in 0|1|2|3|4|5|6|*[!0-9]*|'') ;; *) eval \"exec $fd>&-\" ;; esac; done; exec {}",
            pinned[3].as_raw_fd(), pinned[1].as_raw_fd(), pinned[2].as_raw_fd(), pinned[0].as_raw_fd(),
            args.iter().map(|argument| shell_quote(argument)).collect::<Vec<_>>().join(" "),
        );
        let error = Command::new(BOOTSTRAP_SHELL)
            .args(["-c", &script])
            .env_clear()
            .exec();
        Err(error.to_string())
    }
    struct RunningTree {
        child: std::process::Child,
        wrapper: Option<OwnedFd>,
        init: Option<OwnedFd>,
        reaped: bool,
    }
    impl RunningTree {
        fn request_stop(&self) {
            if let Some(init) = &self.init {
                let _ = rustix::process::pidfd_send_signal(init, rustix::process::Signal::TERM);
            }
        }
        fn force_stop(&mut self) {
            if let Some(init) = &self.init {
                let _ = rustix::process::pidfd_send_signal(init, rustix::process::Signal::KILL);
            }
            if let Some(wrapper) = &self.wrapper {
                let _ = rustix::process::pidfd_send_signal(wrapper, rustix::process::Signal::KILL);
            } else if !self.reaped {
                let _ = self.child.kill();
            }
        }
        fn try_reap(&mut self) -> Result<Option<std::process::ExitStatus>, String> {
            let result = self.child.try_wait().map_err(|error| error.to_string())?;
            self.reaped |= result.is_some();
            Ok(result)
        }
    }
    impl Drop for RunningTree {
        fn drop(&mut self) {
            if self.reaped && self.init.as_ref().is_none_or(pidfd_exited) {
                return;
            }
            if self.init.is_none() {
                self.init = find_init(self.child.id(), None).map(|(_, descriptor, _)| descriptor);
            }
            self.force_stop();
            let deadline = Instant::now() + Duration::from_millis(100);
            while Instant::now() < deadline {
                let _ = self.try_reap();
                if self.reaped && self.init.as_ref().is_some_and(pidfd_exited) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            // No success receipt comes from Drop. Explicit owners must retain
            // unresolved handles and retry finish; this fallback is bounded.
            eprintln!("sandbox host drop: physical exit not confirmed");
        }
    }
    fn nonblocking(descriptor: impl AsFd) -> Result<(), String> {
        let flags = rustix::fs::fcntl_getfl(&descriptor).map_err(|error| error.to_string())?;
        rustix::fs::fcntl_setfl(&descriptor, flags | rustix::fs::OFlags::NONBLOCK)
            .map_err(|error| error.to_string())
    }
    /// The sole physical owner of a long-lived sandbox. Business pipes are
    /// separate from the trusted bootstrap's private control/status pipes.
    #[derive(Clone)]
    pub struct ControlledSandboxStop(std::sync::Arc<std::sync::Mutex<ControlledGate>>);
    struct ControlledGate {
        control: File,
        authenticated: bool,
        stop_at: Option<Instant>,
    }
    impl ControlledSandboxStop {
        /// Stop and GO are serialized on the original private control channel,
        /// independently of the long-lived child/reap lock.
        pub fn request_stop(&self) -> Result<(), String> {
            let mut gate = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            gate.stop_at.get_or_insert_with(Instant::now);
            if !gate.authenticated {
                return Ok(());
            }
            match gate.control.write(b"C") {
                Ok(1) => Ok(()),
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::WouldBlock
                    ) =>
                {
                    Ok(())
                }
                Ok(_) => Err("sandbox stop channel made no progress".into()),
                Err(error) => Err(error.to_string()),
            }
        }
        fn authenticate(&self) {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .authenticated = true;
        }
        fn allow_go(&self) -> Result<(), String> {
            let mut gate = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !gate.authenticated || gate.stop_at.is_some() {
                return Err("sandbox cancelled before GO or host identity unverified".into());
            }
            gate.control
                .write_all(b"G")
                .map_err(|error| error.to_string())
        }
        fn requested_at(&self) -> Option<Instant> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .stop_at
        }
    }
    pub struct ControlledSandbox {
        tree: RunningTree,
        control: ControlledSandboxStop,
        status: File,
        digest: [u8; 32],
        frames: Vec<u8>,
        identity: Option<(u32, u64)>,
        go: bool,
        #[cfg(test)]
        before_go: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
        finished: Option<ControlledHostExit>,
        launch_error: Option<String>,
        pub stdin: Option<File>,
        pub stdout: Option<std::process::ChildStdout>,
        pub stderr: Option<std::process::ChildStderr>,
    }
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ControlledHostExit {
        nonce: [u8; 32],
        wrapper_pid: u32,
        init_pid: u32,
        init_starttime: u64,
        started: bool,
        wrapper_reaped: bool,
        init_exited: bool,
        children_reaped: bool,
    }
    impl ControlledHostExit {
        pub fn authentic_for(&self, nonce: &[u8; 32]) -> bool {
            &self.nonce == nonce
                && self.wrapper_pid != 0
                && self.init_pid != 0
                && self.init_starttime != 0
                && self.wrapper_reaped
                && self.init_exited
                && self.children_reaped
        }
        pub fn host_identity(&self) -> (u32, u32, u64) {
            (self.wrapper_pid, self.init_pid, self.init_starttime)
        }
        pub fn started(&self) -> bool {
            self.started
        }
    }
    impl PreparedSandboxCommand {
        pub fn spawn_controlled(self, nonce: [u8; 32]) -> Result<ControlledSandbox, String> {
            let (stop_read, stop_write) = rustix::pipe::pipe_with(rustix::pipe::PipeFlags::CLOEXEC)
                .map_err(|e| e.to_string())?;
            let (status_read, status_write) =
                rustix::pipe::pipe_with(rustix::pipe::PipeFlags::CLOEXEC)
                    .map_err(|e| e.to_string())?;
            let (input_read, input_write) =
                rustix::pipe::pipe_with(rustix::pipe::PipeFlags::CLOEXEC)
                    .map_err(|e| e.to_string())?;
            let image = File::open(&self.preflight.cowd_binary_path).map_err(|e| e.to_string())?;
            let project = File::open(&self.host_project).map_err(|e| e.to_string())?;
            let (sender, inherited) = UnixDatagram::pair().map_err(|e| e.to_string())?;
            send_capabilities(
                &sender,
                [
                    project.as_fd(),
                    stop_read.as_fd(),
                    status_write.as_fd(),
                    image.as_fd(),
                    input_read.as_fd(),
                ],
            )?;
            let mut args = self.host_args;
            let boundary = args
                .iter()
                .position(|arg| arg == "--")
                .ok_or("sandbox exec boundary missing")?;
            let original = args.split_off(boundary + 1);
            args.pop();
            args.splice(
                0..0,
                ["--cap-drop".into(), "ALL".into(), "--as-pid-1".into()],
            );
            let nonce_hex = nonce
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            args.extend([
                "--".into(),
                INNER_LAUNCHER_PATH.into(),
                INTERNAL_DISPATCH.into(),
                INTERNAL_ROLE.into(),
                "--stream-inner".into(),
                nonce_hex,
            ]);
            args.extend(original);
            let control = File::from(stop_write);
            let status = File::from(status_read);
            let stdin = File::from(input_write);
            nonblocking(&control)?;
            nonblocking(&status)?;
            nonblocking(&stdin)?;
            let inherited: OwnedFd = inherited.into();
            let child = Command::new(format!("/proc/self/fd/{}", image.as_raw_fd()))
                .args([INTERNAL_DISPATCH, INTERNAL_ROLE, "--stream-outer"])
                .arg(&self.preflight.bwrap_path)
                .args(args)
                .env_clear()
                .stdin(Stdio::from(inherited))
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .process_group(0)
                .spawn()
                .map_err(|e| e.to_string())?;
            // From this point every failure is retained in the returned owner,
            // not a String error that could drop an unconfirmed live Child.
            let wrapper = rustix::process::Pid::from_raw(child.id() as i32).and_then(|pid| {
                rustix::process::pidfd_open(pid, rustix::process::PidfdFlags::empty()).ok()
            });
            let mut tree = RunningTree {
                child,
                wrapper,
                init: None,
                reaped: false,
            };
            let stdout = tree.child.stdout.take();
            let stderr = tree.child.stderr.take();
            let mut launch_error = if tree.wrapper.is_none() {
                Some("sandbox wrapper pidfd unavailable".into())
            } else {
                None
            };
            if let Some(pipe) = &stdout {
                if let Err(error) = nonblocking(pipe) {
                    launch_error.get_or_insert(error);
                }
            }
            if let Some(pipe) = &stderr {
                if let Err(error) = nonblocking(pipe) {
                    launch_error.get_or_insert(error);
                }
            }
            Ok(ControlledSandbox {
                tree,
                control: ControlledSandboxStop(std::sync::Arc::new(std::sync::Mutex::new(
                    ControlledGate {
                        control,
                        authenticated: false,
                        stop_at: None,
                    },
                ))),
                status,
                digest: nonce,
                frames: Vec::new(),
                identity: None,
                go: false,
                #[cfg(test)]
                before_go: None,
                finished: None,
                launch_error,
                stdin: Some(stdin),
                stdout,
                stderr,
            })
        }
    }
    impl ControlledSandbox {
        pub fn id(&self) -> u32 {
            self.tree.child.id()
        }
        pub fn try_wait(&mut self) -> Result<Option<std::process::ExitStatus>, String> {
            self.tree.try_reap()
        }
        /// Authentication is OS-derived before GO. No PID from a worker frame
        /// is accepted as a host capability.
        pub fn stop_handle(&self) -> ControlledSandboxStop {
            self.control.clone()
        }
        pub fn allow_go(&mut self, deadline: Instant) -> Result<(), String> {
            if let Some(error) = &self.launch_error {
                return Err(error.clone());
            }
            while self.identity.is_none() {
                if let Some((pid, fd, ticks)) = find_init(self.id(), None) {
                    self.tree.init = Some(fd);
                    self.identity = Some((pid, ticks));
                    self.control.authenticate();
                    break;
                }
                if Instant::now() >= deadline {
                    self.request_stop()?;
                    return Err("sandbox deadline before host authentication".into());
                }
                if self.try_wait()?.is_some() {
                    return Err("sandbox host exited before GO".into());
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            #[cfg(test)]
            if let Some(before_go) = &self.before_go {
                before_go();
            }
            if let Err(error) = self.control.allow_go() {
                self.request_stop()?;
                return Err(error);
            }
            self.go = true;
            while self.frames.len() < FRAME_BYTES {
                self.read_status()?;
                if Instant::now() >= deadline || self.control.requested_at().is_some() {
                    self.request_stop()?;
                    return Err("sandbox start authentication incomplete".into());
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            self.validate_start()
        }
        fn read_status(&mut self) -> Result<(), String> {
            let mut bytes = [0; FRAME_BYTES * 2];
            loop {
                match self.status.read(&mut bytes) {
                    Ok(0) => break,
                    Ok(n) => {
                        if self.frames.len() + n > FRAME_BYTES * 2 {
                            return Err("sandbox host status overflow".into());
                        }
                        self.frames.extend_from_slice(&bytes[..n]);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e.to_string()),
                }
            }
            Ok(())
        }
        fn validate_start(&self) -> Result<(), String> {
            let first: &[u8; FRAME_BYTES] = self
                .frames
                .get(..FRAME_BYTES)
                .ok_or("sandbox start frame missing")?
                .try_into()
                .map_err(|_| "sandbox start frame invalid")?;
            if !valid_frame(first, 1, &self.digest)
                || self
                    .identity
                    .is_none_or(|(_, ticks)| ticks != u64_at(first, 64))
                || first[72] != u8::from(self.go)
            {
                return Err("sandbox host start authentication mismatch".into());
            }
            Ok(())
        }
        pub fn request_stop(&mut self) -> Result<(), String> {
            self.control.request_stop()
        }
        /// Deadline limits waiting; on error this object remains retained by
        /// the original run. No Drop or protocol terminal can mint this fact.
        pub fn finish(&mut self, deadline: Instant) -> Result<ControlledHostExit, String> {
            if let Some(exit) = &self.finished {
                return Ok(exit.clone());
            }
            self.request_stop()?;
            loop {
                if self.identity.is_none() {
                    if let Some((pid, fd, ticks)) = find_init(self.id(), None) {
                        self.identity = Some((pid, ticks));
                        self.tree.init = Some(fd);
                        self.control.authenticate();
                        self.request_stop()?;
                    }
                }
                self.read_status()?;
                let _ = self.try_wait()?;
                let init_exited = self.tree.init.as_ref().is_some_and(pidfd_exited);
                if self.tree.reaped && init_exited {
                    // Namespace init exit is kernel-enforced subtree teardown,
                    // including setsid descendants, independent of worker output.
                    self.validate_start()?;
                    let (init_pid, init_starttime) =
                        self.identity.ok_or("sandbox init identity missing")?;
                    let exit = ControlledHostExit {
                        nonce: self.digest,
                        wrapper_pid: self.id(),
                        init_pid,
                        init_starttime,
                        started: self.go,
                        wrapper_reaped: true,
                        init_exited: true,
                        children_reaped: true,
                    };
                    self.finished = Some(exit.clone());
                    return Ok(exit);
                }
                if self
                    .control
                    .requested_at()
                    .is_some_and(|at| at.elapsed() >= Duration::from_millis(250))
                    && self.identity.is_some()
                {
                    self.tree.force_stop();
                }
                if Instant::now() >= deadline {
                    return Err("sandbox trusted host physical exit remains pending".into());
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        }
    }
    pub(super) fn stream_inner_entry(args: &[String]) -> ExitCode {
        match run_stream_init(args) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("stream init: {error}");
                ExitCode::from(125)
            }
        }
    }
    fn run_stream_init(args: &[String]) -> Result<(), String> {
        if rustix::process::getpid().as_raw_nonzero().get() != 1 || args.len() < 2 {
            return Err("stream requires namespace init".into());
        }
        let mut digest = [0; 32];
        if args[0].len() != 64 || !args[0].is_ascii() {
            return Err("stream nonce invalid".into());
        }
        for (i, byte) in digest.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&args[0][i * 2..i * 2 + 2], 16)
                .map_err(|_| "stream nonce invalid")?;
        }
        let mut control = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open("/proc/self/fd/4")
            .map_err(|e| e.to_string())?;
        let mut status = OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_CLOEXEC)
            .open("/proc/self/fd/5")
            .map_err(|e| e.to_string())?;
        let read = control.metadata().map_err(|e| e.to_string())?;
        let write = status.metadata().map_err(|e| e.to_string())?;
        if !read.file_type().is_fifo() || !write.file_type().is_fifo() || read.ino() == write.ino()
        {
            return Err("stream private channels invalid".into());
        }
        rustix::process::set_dumpable_behavior(rustix::process::DumpableBehavior::NotDumpable)
            .map_err(|e| e.to_string())?;
        let namespace = fs::metadata("/proc/self/ns/pid")
            .map_err(|e| e.to_string())?
            .ino();
        let ticks = process_ticks(1).ok_or("stream init starttime unavailable")?;
        let _tree = NamespaceTree;
        let began = Instant::now();
        let mut go = false;
        while began.elapsed() < Duration::from_secs(10) {
            let mut input = [0];
            match control.read(&mut input) {
                Ok(1) if input[0] == b'G' => {
                    go = true;
                    break;
                }
                Ok(_) => break,
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                    ) =>
                {
                    std::thread::sleep(Duration::from_millis(2))
                }
                Err(e) => return Err(e.to_string()),
            }
        }
        let mut child_pid = None;
        if go {
            let script = format!(
                "exec 3<&-; exec 4<&-; exec 5>&-; exec 6<&-; exec {}",
                args[1..]
                    .iter()
                    .map(|arg| shell_quote(arg))
                    .collect::<Vec<_>>()
                    .join(" ")
            );
            let child = Command::new(BOOTSTRAP_SHELL)
                .args(["-c", &script])
                .stdin(Stdio::inherit())
                .spawn()
                .map_err(|e| e.to_string())?;
            child_pid = Some(child.id());
        }
        status
            .write_all(&frame(
                1, &digest, namespace, ticks, None, None,
                1, // shared codec: start shape; launched=false authenticates no GO
                false, go,
            ))
            .map_err(|e| e.to_string())?;
        let mut exit = None;
        let mut signal = None;
        let mut done = !go;
        let mut stop = if go { 1 } else { 3 };
        loop {
            let mut input = [0];
            match control.read(&mut input) {
                Ok(_) => {
                    stop = 3;
                    done = true;
                }
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                    ) => {}
                Err(_) => {
                    stop = 3;
                    done = true;
                }
            }
            if done {
                kill_descendants();
            }
            match rustix::process::wait(rustix::process::WaitOptions::NOHANG) {
                Ok(Some((pid, result))) => {
                    if Some(pid.as_raw_nonzero().get() as u32) == child_pid {
                        exit = result.exit_status();
                        signal = result.terminating_signal();
                        done = true;
                    }
                }
                Err(rustix::io::Errno::CHILD) => break,
                Ok(None) | Err(rustix::io::Errno::INTR) => {}
                Err(e) => return Err(e.to_string()),
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        status
            .write_all(&frame(
                2, &digest, namespace, ticks, exit, signal, stop, true, go,
            ))
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    impl PreparedBoundedCommand {
        pub fn run(self, cancellation: &AtomicBool) -> BoundedSandboxObservation {
            if cancellation.load(Ordering::Acquire) {
                return empty("bounded_cancelled_before_spawn");
            }
            match self.run_inner(cancellation) {
                Ok(result) => result,
                Err(error) => {
                    eprintln!("bounded start: {error}");
                    empty("bounded_launch_state_unknown")
                }
            }
        }
        fn run_inner(self, cancellation: &AtomicBool) -> Result<BoundedSandboxObservation, String> {
            let state = self.state;
            let timeout = state.deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                return Err("bounded_deadline_before_spawn".into());
            }
            let (cancel_read, cancel_write) =
                rustix::pipe::pipe_with(rustix::pipe::PipeFlags::CLOEXEC)
                    .map_err(|error| error.to_string())?;
            let (status_read, status_write) =
                rustix::pipe::pipe_with(rustix::pipe::PipeFlags::CLOEXEC)
                    .map_err(|error| error.to_string())?;
            let root = canonical(&state.spec.workspace_root).map_err(|error| error.to_string())?;
            let mut args = bwrap_args(
                &state.preflight.bwrap_path,
                &state.preflight.cowd_binary_path,
                &state.spec,
                String::new(),
            )
            .map_err(|error| error.to_string())?;
            let end = args
                .iter()
                .position(|value| value == "--")
                .ok_or("bounded_missing_exec_boundary")?;
            args.truncate(end);
            let root_text = root.to_string_lossy().to_string();
            let mut index = 0;
            while index < args.len() {
                if args[index] == "--ro-bind" && args.get(index + 1) == Some(&root_text) {
                    args[index] = "--ro-bind-fd".into();
                    args[index + 1] = "6".into();
                    args[index + 2] = "/project".into();
                    index += 3;
                } else if args[index] == "--chdir" {
                    args[index + 1] = "/project".into();
                    index += 2;
                } else if args[index] == "--tmpfs"
                    && args.get(index + 1).is_some_and(|path| path == "/tmp")
                {
                    args.splice(index..index, ["--size".into(), "33554432".into()]);
                    index += 4;
                } else {
                    index += 1;
                }
            }
            args.splice(
                0..0,
                ["--cap-drop".into(), "ALL".into(), "--as-pid-1".into()],
            );
            let hex = state
                .digest
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            args.extend([
                "--".into(),
                INNER_LAUNCHER_PATH.into(),
                INTERNAL_DISPATCH.into(),
                INTERNAL_ROLE.into(),
                "--bounded-inner".into(),
                hex,
                state.command,
            ]);
            let (handoff, inherited) = UnixDatagram::pair().map_err(|error| error.to_string())?;
            send_capabilities(&handoff, [
                state.project.as_fd(), cancel_read.as_fd(), status_write.as_fd(), state.image.as_fd(),
            ])?;
            let inherited: OwnedFd = inherited.into();
            let mut child = match Command::new(format!("/proc/self/fd/{}", state.image.as_raw_fd()))
                .args([INTERNAL_DISPATCH, INTERNAL_ROLE, "--bounded-outer"])
                .arg(&state.preflight.bwrap_path)
                .args(args)
                .env_clear()
                .stdin(Stdio::from(inherited))
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
            {
                Ok(child) => child,
                Err(error) => {
                    eprintln!("bounded spawn: {error}");
                    return Ok(empty("bounded_launcher_start_failed"));
                }
            };
            drop(handoff);
            drop(cancel_read);
            drop(status_write);
            let pid = rustix::process::Pid::from_raw(child.id() as i32)
                .ok_or("bounded_wrapper_pid_invalid")?;
            let wrapper =
                match rustix::process::pidfd_open(pid, rustix::process::PidfdFlags::empty()) {
                    Ok(descriptor) => descriptor,
                    Err(error) => {
                        let _ = child.kill();
                        let _ = child.wait();
                        return Err(error.to_string());
                    }
                };
            let mut tree = RunningTree {
                child,
                wrapper: Some(wrapper),
                init: None,
                reaped: false,
            };
            let mut stdout = tree.child.stdout.take().ok_or("bounded_stdout_missing")?;
            let mut stderr = tree.child.stderr.take().ok_or("bounded_stderr_missing")?;
            let mut control = File::from(status_read);
            let mut cancel = File::from(cancel_write);
            nonblocking(&stdout)?;
            nonblocking(&stderr)?;
            nonblocking(&control)?;
            nonblocking(&cancel)?;
            let mut result = empty("");
            result.state.complete = true;
            let limit = env::var("COWD_EXEC_MAX_OUTPUT_KIB")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .map(|value| value.saturating_mul(1024))
                .unwrap_or(CAPTURE)
                .min(CAPTURE);
            let mut frames = Vec::with_capacity(FRAME_BYTES * 2);
            let mut start: Option<[u8; FRAME_BYTES]> = None;
            let mut terminal: Option<[u8; FRAME_BYTES]> = None;
            let mut go = false;
            let mut stopped: Option<Instant> = None;
            let mut reaped_at: Option<Instant> = None;
            let mut stdout_eof = false;
            let mut stderr_eof = false;
            let began = Instant::now();
            loop {
                let mut stop = None;
                if tree.init.is_none() {
                    if let Some((pid, descriptor, ticks)) = find_init(tree.child.id(), None) {
                        tree.init = Some(descriptor);
                        result.state.identity = Some((0, pid, ticks));
                        if !cancellation.load(Ordering::Acquire) && Instant::now() < state.deadline
                        {
                            go = true;
                            if cancel.write_all(b"G").is_err() {
                                result.state.reason = "bounded_handshake_failed".into();
                                stop = Some(b'C');
                            }
                        } else {
                            stop = Some(if cancellation.load(Ordering::Acquire) {
                                b'C'
                            } else {
                                b'T'
                            });
                        }
                    }
                }
                for (stream, output, eof) in [
                    (
                        &mut stdout as &mut dyn Read,
                        &mut result.state.stdout,
                        &mut stdout_eof,
                    ),
                    (
                        &mut stderr as &mut dyn Read,
                        &mut result.state.stderr,
                        &mut stderr_eof,
                    ),
                ] {
                    match drain(stream, output, limit) {
                        Ok((overflow, ended)) => {
                            *eof |= ended;
                            if overflow {
                                result.state.complete = false;
                                stop = Some(b'L');
                            }
                        }
                        Err(_) => {
                            *eof = true;
                            result.state.complete = false;
                            result.state.reason = "bounded_reader_failed".into();
                            stop = Some(b'C');
                        }
                    }
                }
                let mut buffer = [0; FRAME_BYTES * 2];
                match control.read(&mut buffer) {
                    Ok(count) if count > 0 => {
                        if frames.len() + count > FRAME_BYTES * 2 {
                            result.state.reason = "bounded_control_overflow".into();
                            stop = Some(b'C');
                        } else {
                            frames.extend_from_slice(&buffer[..count]);
                        }
                    }
                    Err(error)
                        if !matches!(
                            error.kind(),
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                        ) =>
                    {
                        result.state.reason = "bounded_control_failed".into();
                        stop = Some(b'C');
                    }
                    _ => {}
                }
                if start.is_none() && frames.len() >= FRAME_BYTES {
                    let mut frame = [0; FRAME_BYTES];
                    frame.copy_from_slice(&frames[..FRAME_BYTES]);
                    if !valid_frame(&frame, 1, &state.digest)
                        || result
                            .state
                            .identity
                            .is_none_or(|(_, _, ticks)| ticks != u64_at(&frame, 64))
                    {
                        result.state.reason = "bounded_started_frame_invalid".into();
                        stop = Some(b'C');
                    } else if let Some((_, pid, ticks)) = result.state.identity {
                        result.state.identity = Some((u64_at(&frame, 44), pid, ticks));
                        start = Some(frame);
                        result.state.protected = true;
                        result.state.started = frame[72] == 1;
                    }
                }
                if terminal.is_none() && frames.len() == FRAME_BYTES * 2 {
                    let mut frame = [0; FRAME_BYTES];
                    frame.copy_from_slice(&frames[FRAME_BYTES..]);
                    if valid_frame(&frame, 2, &state.digest)
                        && start.as_ref().is_some_and(|first| {
                            first[44..56] == frame[44..56]
                                && first[64..72] == frame[64..72]
                                && first[72] == frame[72]
                        })
                    {
                        terminal = Some(frame);
                    } else {
                        result.state.reason = "bounded_terminal_frame_invalid".into();
                        stop = Some(b'C');
                    }
                }
                if cancellation.load(Ordering::Acquire) {
                    stop = Some(b'C');
                }
                if Instant::now() >= state.deadline {
                    stop = Some(b'T');
                }
                if !go && began.elapsed() >= Duration::from_secs(10) {
                    result.state.reason = "bounded_startup_deadline".into();
                    stop = Some(b'C');
                }
                if let Some(reason) = stop.filter(|_| stopped.is_none()) {
                    let _ = cancel.write_all(&[reason]);
                    stopped = Some(Instant::now());
                    result.state.stop = match reason {
                        b'T' => "timeout",
                        b'L' => "capture_limit",
                        _ => "cancelled",
                    };
                }
                if stopped.is_some_and(|instant| instant.elapsed() >= Duration::from_secs(5))
                    && !tree.reaped
                {
                    result.state.reason = "bounded_reap_deadline".into();
                    if let Some(init) = &tree.init {
                        let _ =
                            rustix::process::pidfd_send_signal(init, rustix::process::Signal::KILL);
                    }
                    tree.force_stop();
                }
                match tree.child.try_wait() {
                    Ok(Some(status)) => {
                        tree.reaped = true;
                        let reaped = *reaped_at.get_or_insert_with(Instant::now);
                        if frames.len() < FRAME_BYTES * 2
                            && reaped.elapsed() < Duration::from_millis(100)
                        {
                            std::thread::sleep(Duration::from_millis(2));
                            continue;
                        }
                        if (!stdout_eof || !stderr_eof) && reaped.elapsed() < Duration::from_secs(5)
                        {
                            std::thread::sleep(Duration::from_millis(2));
                            continue;
                        }
                        if !stdout_eof || !stderr_eof {
                            result.state.complete = false;
                            result.state.reason = "bounded_output_drain_incomplete".into();
                        }
                        if let Some(frame) = terminal {
                            result.state.exit =
                                (i32_at(&frame, 56) >= 0).then(|| i32_at(&frame, 56));
                            result.state.signal =
                                (i32_at(&frame, 60) > 0).then(|| i32_at(&frame, 60));
                            result.state.children_reaped = frame[11] & 128 != 0;
                            result.state.started = frame[72] == 1;
                            if frame[11] & 16 != 0 {
                                result.state.reason = "bounded_cleanup_deadline".into();
                            }
                            result.state.stop = match frame[10] {
                                1 => "command_exit",
                                2 => "timeout",
                                3 => "cancelled",
                                4 => "capture_limit",
                                _ => {
                                    result.state.reason = "bounded_stop_invalid".into();
                                    "command_exit"
                                }
                            };
                            result.state.init_reaped =
                                status.success() && tree.init.as_ref().is_some_and(pidfd_exited);
                            if result.state.started {
                                result.state.profile = Some(SandboxLaunchReceipt {
                                    backend: "bubblewrap+landlock+seccomp+pid1",
                                    workspace_access: SandboxWorkspaceAccess::ReadOnly,
                                    network_enabled: false,
                                    kernel_hardening_applied: true,
                                    protected_roots_denied: true,
                                    inherited_fds_closed: true,
                                    environment_allowlist_enforced: true,
                                    prepared_at_ms: now_ms(),
                                });
                            } else if result.state.reason.is_empty() {
                                result.state.reason = "bounded_not_started".into();
                            }
                        } else {
                            result.state.reason = "bounded_terminal_missing".into();
                        }
                        if frames.len() != FRAME_BYTES * 2 || start.is_none() {
                            result.state.reason = "bounded_control_incomplete".into();
                        }
                        return Ok(result);
                    }
                    Ok(None) => {}
                    Err(_) => {
                        result.state.reason = "bounded_wrapper_wait_failed".into();
                        if stopped.is_none() {
                            let _ = cancel.write_all(b"C");
                            stopped = Some(Instant::now());
                        }
                    }
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        }
    }
    fn pidfd_exited(descriptor: &OwnedFd) -> bool {
        let mut descriptors = [rustix::event::PollFd::new(
            descriptor,
            rustix::event::PollFlags::IN,
        )];
        rustix::event::poll(
            &mut descriptors,
            Some(&rustix::event::Timespec {
                tv_sec: 0,
                tv_nsec: 0,
            }),
        )
        .is_ok_and(|count| count > 0)
    }
    fn drain(
        stream: &mut dyn Read,
        output: &mut Vec<u8>,
        limit: usize,
    ) -> std::io::Result<(bool, bool)> {
        let mut buffer = [0; 8192];
        for _ in 0..32 {
            match stream.read(&mut buffer) {
                Ok(0) => return Ok((false, true)),
                Ok(count) => {
                    let available = limit.saturating_sub(output.len());
                    output.extend_from_slice(&buffer[..count.min(available)]);
                    if count > available {
                        return Ok((true, false));
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    return Ok((false, false))
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        Ok((false, false))
    }
    fn u64_at(frame: &[u8; FRAME_BYTES], offset: usize) -> u64 {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(&frame[offset..offset + 8]);
        u64::from_le_bytes(bytes)
    }
    fn i32_at(frame: &[u8; FRAME_BYTES], offset: usize) -> i32 {
        let mut bytes = [0; 4];
        bytes.copy_from_slice(&frame[offset..offset + 4]);
        i32::from_le_bytes(bytes)
    }
    fn valid_frame(frame: &[u8; FRAME_BYTES], sequence: u8, digest: &[u8; 32]) -> bool {
        &frame[..8] == MAGIC
            && frame[8] == 1
            && frame[9] == sequence
            && frame[12..44] == digest[..]
            && frame[11] & 15 == 15
            && frame[52..56] == 1_u32.to_le_bytes()
            && frame[73..].iter().all(|byte| *byte == 0)
            && u64_at(frame, 44) != 0
            && u64_at(frame, 64) != 0
            && if sequence == 1 {
                frame[10] == 1
                    && frame[11] == 15
                    && frame[72] <= 1
                    && i32_at(frame, 56) == -1
                    && i32_at(frame, 60) == 0
            } else {
                (1..=4).contains(&frame[10])
                    && matches!(frame[11], 143 | 159)
                    && frame[72] <= 1
                    && (frame[72] == 0 || ((i32_at(frame, 56) >= 0) != (i32_at(frame, 60) > 0)))
            }
    }
    fn process_ticks(pid: u32) -> Option<u64> {
        let text = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        text.rsplit_once(") ")?
            .1
            .split_whitespace()
            .nth(19)?
            .parse()
            .ok()
    }
    fn find_init(wrapper: u32, expected_ticks: Option<u64>) -> Option<(u32, OwnedFd, u64)> {
        let mut pending = vec![wrapper];
        let mut seen = BTreeSet::new();
        while let Some(pid) = pending.pop() {
            if !seen.insert(pid) || seen.len() > 256 {
                continue;
            }
            let status = fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
            let nested_init = status
                .lines()
                .find(|line| line.starts_with("NSpid:"))
                .is_some_and(|line| {
                    let ids = line.split_whitespace().skip(1).collect::<Vec<_>>();
                    ids.len() >= 2 && ids.last() == Some(&"1")
                });
            if nested_init
                && process_ticks(pid)
                    .is_some_and(|ticks| expected_ticks.is_none_or(|expected| expected == ticks))
            {
                let ticks = process_ticks(pid)?;
                let descriptor = rustix::process::pidfd_open(
                    rustix::process::Pid::from_raw(pid as i32)?,
                    rustix::process::PidfdFlags::empty(),
                )
                .ok()?;
                if process_ticks(pid) == Some(ticks) {
                    return Some((pid, descriptor, ticks));
                }
            }
            if let Ok(children) = fs::read_to_string(format!("/proc/{pid}/task/{pid}/children")) {
                pending.extend(
                    children
                        .split_whitespace()
                        .filter_map(|value| value.parse::<u32>().ok()),
                );
            }
        }
        None
    }
    fn frame(
        sequence: u8,
        digest: &[u8; 32],
        namespace: u64,
        ticks: u64,
        exit: Option<i32>,
        signal: Option<i32>,
        stop: u8,
        reaped: bool,
        launched: bool,
    ) -> [u8; FRAME_BYTES] {
        let mut frame = [0; FRAME_BYTES];
        frame[..8].copy_from_slice(MAGIC);
        frame[8] = 1;
        frame[9] = sequence;
        frame[10] = stop;
        frame[11] = 15 | if reaped { 128 } else { 0 };
        frame[12..44].copy_from_slice(digest);
        frame[44..52].copy_from_slice(&namespace.to_le_bytes());
        frame[52..56].copy_from_slice(&1_u32.to_le_bytes());
        frame[56..60].copy_from_slice(&exit.unwrap_or(-1).to_le_bytes());
        frame[60..64].copy_from_slice(&signal.unwrap_or(0).to_le_bytes());
        frame[64..72].copy_from_slice(&ticks.to_le_bytes());
        frame[72] = u8::from(launched);
        frame
    }
    struct NamespaceTree;
    impl Drop for NamespaceTree {
        fn drop(&mut self) {
            loop {
                kill_descendants();
                match rustix::process::wait(rustix::process::WaitOptions::NOHANG) {
                    Err(rustix::io::Errno::CHILD) => break,
                    _ => std::thread::sleep(Duration::from_millis(2)),
                }
            }
        }
    }
    fn kill_descendants() {
        if let Ok(entries) = fs::read_dir("/proc") {
            for entry in entries.filter_map(Result::ok) {
                let Some(pid) = entry
                    .file_name()
                    .to_str()
                    .and_then(|value| value.parse::<i32>().ok())
                    .filter(|pid| *pid > 1)
                    .and_then(rustix::process::Pid::from_raw)
                else {
                    continue;
                };
                if let Ok(descriptor) =
                    rustix::process::pidfd_open(pid, rustix::process::PidfdFlags::empty())
                {
                    let _ = rustix::process::pidfd_send_signal(
                        &descriptor,
                        rustix::process::Signal::KILL,
                    );
                }
            }
        }
    }
    pub(super) fn inner_entry(args: &[String]) -> ExitCode {
        match run_init(args) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("bounded init: {error}");
                ExitCode::from(125)
            }
        }
    }
    fn run_init(args: &[String]) -> Result<(), String> {
        if rustix::process::getpid().as_raw_nonzero().get() != 1 || args.len() != 2 {
            return Err("bounded_requires_namespace_pid1".into());
        }
        let mut digest = [0; 32];
        if args[0].len() != 64 || !args[0].is_ascii() {
            return Err("bounded_digest_invalid".into());
        }
        for (index, byte) in digest.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&args[0][index * 2..index * 2 + 2], 16)
                .map_err(|_| "bounded_digest_invalid")?;
        }
        let mut control = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open("/proc/self/fd/4")
            .map_err(|error| error.to_string())?;
        let mut status = OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_CLOEXEC)
            .open("/proc/self/fd/5")
            .map_err(|error| error.to_string())?;
        let control_metadata = control.metadata().map_err(|error| error.to_string())?;
        let status_metadata = status.metadata().map_err(|error| error.to_string())?;
        if !control_metadata.file_type().is_fifo()
            || !status_metadata.file_type().is_fifo()
            || control_metadata.ino() == status_metadata.ino()
        {
            return Err("bounded_private_pipe_invalid".into());
        }
        rustix::process::set_dumpable_behavior(rustix::process::DumpableBehavior::NotDumpable)
            .map_err(|error| error.to_string())?;
        if rustix::process::dumpable_behavior().map_err(|error| error.to_string())?
            != rustix::process::DumpableBehavior::NotDumpable
        {
            return Err("bounded_dumpable_not_disabled".into());
        }
        install_landlock(&[PathBuf::from("/tmp"), PathBuf::from("/dev")])?;
        install_seccomp(true)?;
        let mounts =
            fs::read_to_string("/proc/self/mountinfo").map_err(|error| error.to_string())?;
        if !mounts.lines().any(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            fields.get(4) == Some(&"/project")
                && fields
                    .get(5)
                    .is_some_and(|options| options.split(',').any(|option| option == "ro"))
        }) {
            return Err("bounded_project_mount_not_read_only".into());
        }
        let interfaces = fs::read_to_string("/proc/net/dev").map_err(|error| error.to_string())?;
        let temporary = rustix::fs::statvfs("/tmp").map_err(|error| error.to_string())?;
        if (temporary.f_blocks as u64).saturating_mul(temporary.f_frsize as u64) > 33554432 {
            return Err("bounded_private_tmpfs_limit_missing".into());
        }
        if interfaces.lines().skip(2).any(|line| {
            line.split(':')
                .next()
                .is_some_and(|name| name.trim() != "lo")
        }) {
            return Err("bounded_network_namespace_not_private".into());
        }
        let rules = [libc::SYS_setns, libc::SYS_unshare, libc::SYS_pidfd_getfd]
            .into_iter()
            .map(|syscall| (syscall, Vec::new()))
            .collect::<BTreeMap<_, _>>();
        let filter: BpfProgram = SeccompFilter::new(
            rules,
            SeccompAction::Allow,
            SeccompAction::Trap,
            std::env::consts::ARCH
                .try_into()
                .map_err(|error| format!("{error}"))?,
        )
        .map_err(|error| error.to_string())?
        .try_into()
        .map_err(|error: seccompiler::BackendError| error.to_string())?;
        seccompiler::apply_filter(&filter).map_err(|error| error.to_string())?;
        if !rustix::thread::no_new_privs().map_err(|error| error.to_string())? {
            return Err("bounded_no_new_privs_missing".into());
        }
        let security =
            fs::read_to_string("/proc/self/status").map_err(|error| error.to_string())?;
        for key in ["CapEff:", "CapPrm:", "CapInh:", "CapAmb:"] {
            if !security
                .lines()
                .find(|line| line.starts_with(key))
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|value| u64::from_str_radix(value, 16).ok())
                .is_some_and(|capabilities| capabilities == 0)
            {
                return Err("bounded_untrusted_capability_ceiling".into());
            }
        }
        let namespace = fs::metadata("/proc/self/ns/pid")
            .map_err(|error| error.to_string())?
            .ino();
        let ticks = process_ticks(1).ok_or("bounded_init_ticks_missing")?;
        let _tree = NamespaceTree;
        let began = Instant::now();
        let mut stop = 1;
        loop {
            let mut input = [0];
            match control.read(&mut input) {
                Ok(1) if input[0] == b'G' => break,
                Ok(1) => {
                    stop = match input[0] {
                        b'T' => 2,
                        b'L' => 4,
                        _ => 3,
                    };
                    break;
                }
                Ok(0) => {
                    stop = 3;
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if began.elapsed() >= Duration::from_secs(10) {
                        stop = 3;
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                _ => {
                    stop = 3;
                    break;
                }
            }
        }
        let mut command_pid = None;
        if stop == 1 {
            let script = format!(
                "exec 3<&-; exec 4<&-; exec 5>&-; exec {} {} {} --bounded-child {}",
                shell_quote(INNER_LAUNCHER_PATH),
                shell_quote(INTERNAL_DISPATCH),
                shell_quote(INTERNAL_ROLE),
                shell_quote(&args[1])
            );
            let child = Command::new(BOOTSTRAP_SHELL)
                .arg("-c")
                .arg(script)
                .stdin(Stdio::null())
                .spawn()
                .map_err(|error| error.to_string())?;
            command_pid = Some(child.id());
        }
        status
            .write_all(&frame(
                1,
                &digest,
                namespace,
                ticks,
                None,
                None,
                1,
                false,
                command_pid.is_some(),
            ))
            .map_err(|error| error.to_string())?;
        let mut exit = None;
        let mut signal = None;
        let mut done = command_pid.is_none();
        let mut cleanup_at: Option<Instant> = None;
        loop {
            let mut input = [0];
            match control.read(&mut input) {
                Ok(1) => {
                    stop = match input[0] {
                        b'T' => 2,
                        b'L' => 4,
                        _ => 3,
                    };
                    kill_descendants();
                }
                Ok(0) => {
                    stop = 3;
                    kill_descendants();
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                    ) => {}
                _ => {
                    stop = 3;
                    kill_descendants();
                }
            }
            match rustix::process::wait(rustix::process::WaitOptions::NOHANG) {
                Ok(Some((pid, result))) => {
                    if !done && Some(pid.as_raw_nonzero().get() as u32) == command_pid {
                        exit = result.exit_status();
                        signal = result.terminating_signal();
                        done = true;
                    }
                }
                Err(rustix::io::Errno::CHILD) => break,
                Err(rustix::io::Errno::INTR) | Ok(None) => {}
                Err(error) => return Err(error.to_string()),
            }
            if done || stop != 1 {
                cleanup_at.get_or_insert_with(Instant::now);
                kill_descendants();
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        let mut terminal = frame(
            2,
            &digest,
            namespace,
            ticks,
            exit,
            signal,
            stop,
            true,
            command_pid.is_some(),
        );
        if cleanup_at.is_some_and(|instant| instant.elapsed() >= Duration::from_secs(5)) {
            terminal[11] |= 16;
        }
        status
            .write_all(&terminal)
            .map_err(|error| error.to_string())?;
        Ok(())
    }
    pub(super) fn child_entry(args: &[String]) -> ExitCode {
        if args.len() != 1 {
            return ExitCode::from(64);
        }
        use rustix::process::{getrlimit, setrlimit, Resource, Rlimit};
        for (resource, ceiling) in [
            (Resource::Nproc, 128),
            (Resource::As, 536870912),
            (Resource::Nofile, 64),
            (Resource::Fsize, 16777216),
            (Resource::Core, 0),
        ] {
            let previous = getrlimit(resource);
            let maximum = previous.maximum.map_or(ceiling, |value| value.min(ceiling));
            let current = previous.current.map_or(maximum, |value| value.min(maximum));
            if setrlimit(
                resource,
                Rlimit {
                    current: Some(current),
                    maximum: Some(maximum),
                },
            )
            .is_err()
            {
                return ExitCode::from(125);
            }
        }
        let error = Command::new("/bin/sh").arg("-c").arg(&args[0]).exec();
        eprintln!("bounded command exec: {error}");
        ExitCode::from(125)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        struct Project(PathBuf);
        impl Project {
            fn new() -> Self {
                static SEQUENCE: std::sync::atomic::AtomicU64 =
                    std::sync::atomic::AtomicU64::new(0);
                let path = env::temp_dir().join(format!(
                    "cowd-bounded-launch-tests-{}-{}-{}",
                    std::process::id(),
                    now_ms(),
                    SEQUENCE.fetch_add(1, Ordering::Relaxed)
                ));
                fs::create_dir(&path).unwrap();
                Self(path)
            }
            fn spec(&self) -> SandboxLaunchSpec {
                let mut spec = SandboxLaunchSpec::workspace(&self.0);
                spec.workspace_access = SandboxWorkspaceAccess::ReadOnly;
                spec.network_enabled = false;
                spec
            }
        }
        impl Drop for Project {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        fn command(project: &Project, source: &str, timeout: Duration) -> PreparedBoundedCommand {
            prepare_bounded_command(
                project.spec(),
                format!("exec python3 -c {}", shell_quote(source)),
                timeout,
                [7; 32],
            )
            .unwrap()
        }
        fn assert_settled(result: &BoundedSandboxObservation) {
            assert!(result.started(), "{}", result.reason_code());
            assert_eq!(result.reason_code(), "");
            assert!(result.all_children_reaped());
            assert!(result.init_reaped());
            assert!(result.status_channel_protected());
            assert!(result
                .identity()
                .is_some_and(|(namespace, pid, ticks)| namespace != 0 && pid > 1 && ticks != 0));
            assert!(result
                .applied_profile()
                .is_some_and(
                    |profile| profile.backend == "bubblewrap+landlock+seccomp+pid1"
                        && !profile.network_enabled
                        && profile.workspace_access == SandboxWorkspaceAccess::ReadOnly
                        && profile.kernel_hardening_applied
                ));
        }

        #[test]
        fn r1_702_bounded_non_dumpable_parent_high_fd_and_private_handoff() {
            const CHILD: &str = "COWD_702_NON_DUMPABLE_CHILD";
            if env::var_os(CHILD).is_none() {
                let mut child = Command::new(env::current_exe().unwrap())
                    .args(["--exact", "bounded::tests::r1_702_bounded_non_dumpable_parent_high_fd_and_private_handoff", "--nocapture"])
                    .env(CHILD, "1")
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap();
                let deadline = Instant::now() + Duration::from_secs(45);
                loop {
                    if let Some(status) = child.try_wait().unwrap() {
                        let output = child.wait_with_output().unwrap();
                        assert!(status.success(), "{}", String::from_utf8_lossy(&output.stderr));
                        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
                        break;
                    }
                    if Instant::now() >= deadline {
                        child.kill().unwrap();
                        let _ = child.wait();
                        panic!("bounded non-dumpable production child exceeded its deadline");
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
                return;
            }
            rustix::process::set_dumpable_behavior(rustix::process::DumpableBehavior::NotDumpable).unwrap();
            let project = Project::new();
            fs::write(project.0.join("input.txt"), "one\ntwo\n").unwrap();
            let source = r#"
import os
assert open('/project/input.txt').read().count('\n') == 2
for entry in os.listdir('/proc/self/fd'):
    descriptor = int(entry)
    if descriptor > 2:
        try: os.fstat(descriptor)
        except OSError: continue
        raise AssertionError('leaked private descriptor')
assert 'COWD_702_NON_DUMPABLE_CHILD' not in os.environ
print('production-payload-complete')
"#;
            let mut prepared = command(&project, source, Duration::from_secs(15));
            let high = rustix::io::fcntl_dupfd_cloexec(&prepared.state.project, 1025).unwrap();
            prepared.state.project = File::from(high);
            assert!(prepared.state.project.as_raw_fd() >= 1025);
            let observer = prepared.state.project.try_clone().unwrap();
            let result = prepared.run(&AtomicBool::new(false));
            assert_settled(&result);
            assert_eq!(result.exit_code(), Some(0));
            assert_eq!(result.stdout(), b"production-payload-complete\n");
            assert!(rustix::io::fcntl_getfd(&observer)
                .unwrap()
                .contains(rustix::io::FdFlags::CLOEXEC));
            assert_eq!(
                rustix::process::dumpable_behavior().unwrap(),
                rustix::process::DumpableBehavior::NotDumpable
            );
        }

        #[test]
        fn controlled_stop_wins_between_authentication_and_go_without_child_lock() {
            let project = Project::new();
            let prepared = shell_command("printf forbidden-business", &project.spec()).unwrap();
            let nonce = [61; 32];
            let mut host = prepared.spawn_controlled(nonce).unwrap();
            let stop = host.stop_handle();
            let (entered, authenticated) = std::sync::mpsc::channel();
            let (release, proceed) = std::sync::mpsc::channel();
            let proceed = std::sync::Mutex::new(proceed);
            host.before_go = Some(std::sync::Arc::new(move || {
                entered.send(()).unwrap();
                proceed
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            }));
            let runner = std::thread::spawn(move || {
                let started = host.allow_go(Instant::now() + Duration::from_secs(10));
                (host, started)
            });
            authenticated.recv_timeout(Duration::from_secs(5)).unwrap();
            // The runner owns the complete host, as allow_go does under the
            // adapter child lock. Stop still serializes before the actual G byte.
            stop.request_stop().unwrap();
            release.send(()).unwrap();
            let (mut host, started) = runner.join().unwrap();
            assert!(started.unwrap_err().contains("cancelled before GO"));
            let exit = host
                .finish(Instant::now() + Duration::from_secs(5))
                .unwrap();
            assert!(exit.authentic_for(&nonce));
            assert!(!exit.started());
            let mut stdout = host.stdout.take().unwrap();
            let mut observed = Vec::new();
            let (_, eof) = drain(&mut stdout, &mut observed, 4096).unwrap();
            assert!(eof);
            assert!(observed.is_empty(), "cancel won but business ran");
        }

        #[test]
        fn controlled_host_go_gate_and_detached_pipe_descendants_have_one_exit_owner() {
            for cancelled in [true, false] {
                let project = Project::new();
                let nonce = [41; 32];
                let command = if cancelled {
                    "printf forbidden-business"
                } else {
                    "python3 -c 'import os,time; p=os.fork(); os.setsid() if p==0 else None; print(\"detached-ready\",flush=True) if p==0 else None; time.sleep(600)'"
                };
                let prepared = shell_command(command, &project.spec()).unwrap();
                let mut host = prepared.spawn_controlled(nonce).unwrap();
                if cancelled {
                    host.stop_handle().request_stop().unwrap();
                }
                let started = host.allow_go(Instant::now() + Duration::from_secs(10));
                assert_eq!(started.is_err(), cancelled);
                let mut stdout = host.stdout.take().unwrap();
                let mut stderr = host.stderr.take().unwrap();
                if !cancelled {
                    let deadline = Instant::now() + Duration::from_secs(5);
                    let mut entered = Vec::new();
                    loop {
                        let _ = drain(&mut stdout, &mut entered, 4096).unwrap();
                        if entered
                            .windows(b"detached-ready".len())
                            .any(|window| window == b"detached-ready")
                        {
                            break;
                        }
                        assert!(
                            Instant::now() < deadline,
                            "setsid descendant handshake not hit"
                        );
                        std::thread::sleep(Duration::from_millis(2));
                    }
                }
                let exit = host
                    .finish(Instant::now() + Duration::from_secs(5))
                    .unwrap();
                assert!(exit.authentic_for(&nonce));
                assert!(!exit.authentic_for(&[42; 32]));
                assert_eq!(exit.started(), !cancelled);
                assert_eq!(
                    host.finish(Instant::now() + Duration::from_secs(1))
                        .unwrap(),
                    exit
                );
                let mut out = Vec::new();
                let mut err = Vec::new();
                let (_, stdout_eof) = drain(&mut stdout, &mut out, 4096).unwrap();
                let (_, stderr_eof) = drain(&mut stderr, &mut err, 4096).unwrap();
                assert!(
                    stdout_eof && stderr_eof,
                    "namespace init exit must end descendant pipes"
                );
                if cancelled {
                    assert!(
                        out.is_empty(),
                        "GO-before-cancel cannot execute business content"
                    );
                }
            }
        }

        #[test]
        fn r1_702_bounded_mount_stays_pinned_across_path_replacement() {
            let project = Project::new();
            fs::write(project.0.join("input.txt"), "original").unwrap();
            let prepared = command(
                &project,
                "assert open('/project/input.txt').read() == 'original'; print('pinned')",
                Duration::from_secs(15),
            );
            let replacement = Project::new();
            let original = replacement.0.join("original");
            fs::rename(&project.0, &original).unwrap();
            fs::create_dir(&project.0).unwrap();
            fs::write(project.0.join("input.txt"), "replacement").unwrap();
            let result = prepared.run(&AtomicBool::new(false));
            assert_settled(&result);
            assert_eq!(result.exit_code(), Some(0));
            assert_eq!(result.stdout(), b"pinned\n");
        }

        #[test]
        fn r1_702_handoff_rejects_missing_truncated_and_wrong_access_capabilities() {
            let project = Project::new();
            let directory = File::open(&project.0).unwrap();
            let image = File::open(env::current_exe().unwrap()).unwrap();
            let (cancel_read, cancel_write) = rustix::pipe::pipe_with(rustix::pipe::PipeFlags::CLOEXEC).unwrap();
            let (status_read, status_write) = rustix::pipe::pipe_with(rustix::pipe::PipeFlags::CLOEXEC).unwrap();
            let (sender, receiver) = UnixDatagram::pair().unwrap();
            assert!(receive_capabilities(&receiver).is_err());
            sender.send(b"CWDFD702").unwrap();
            assert!(receive_capabilities(&receiver).is_err());
            send_capabilities(&sender, [directory.as_fd(), cancel_write.as_fd(), status_read.as_fd(), image.as_fd()]).unwrap();
            assert!(receive_capabilities(&receiver).is_err());
            let descriptors = [directory.as_fd(), cancel_read.as_fd(), status_write.as_fd(), image.as_fd(), image.as_fd()];
            let mut space = [std::mem::MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(5))];
            let mut ancillary = rustix::net::SendAncillaryBuffer::new(&mut space);
            assert!(ancillary.push(rustix::net::SendAncillaryMessage::ScmRights(&descriptors)));
            rustix::net::sendmsg(&sender, &[std::io::IoSlice::new(b"CWDFD702")], &mut ancillary, rustix::net::SendFlags::NOSIGNAL).unwrap();
            assert!(receive_capabilities(&receiver).is_err());
            send_capabilities(&sender, [directory.as_fd(), cancel_read.as_fd(), status_write.as_fd(), image.as_fd()]).unwrap();
            let received = receive_capabilities(&receiver).unwrap();
            assert_eq!(received.len(), 4);
            assert!(received.iter().all(|descriptor| rustix::io::fcntl_getfd(descriptor).unwrap().contains(rustix::io::FdFlags::CLOEXEC)));
            assert_eq!(rustix::fs::fstat(&received[0]).unwrap().st_ino, directory.metadata().unwrap().ino());
        }

        #[test]
        fn r1_702_bounded_ordinary_nonzero_is_not_unknown_or_success() {
            let project = Project::new();
            let prepared = command(&project, "raise SystemExit(7)", Duration::from_secs(15));
            let result = prepared.run(&AtomicBool::new(false));
            assert_settled(&result);
            assert_eq!(result.exit_code(), Some(7));
            assert_eq!(result.signal(), None);
            assert_eq!(result.stop_reason(), "command_exit");
            let prepared = prepare_bounded_command(project.spec(), "exec /missing-702-program".into(), Duration::from_secs(15), [7; 32]).unwrap();
            let result = prepared.run(&AtomicBool::new(false));
            assert_settled(&result);
            assert_ne!(result.exit_code(), Some(0));
        }

        #[test]
        fn r1_702_bounded_sigterm_reaps_detached_tree_and_preserves_signal() {
            let project = Project::new();
            let prepared = command(&project, "import os,signal,time\nif os.fork()==0:\n os.setsid(); time.sleep(600); os._exit(0)\nos.kill(os.getpid(), signal.SIGTERM)", Duration::from_secs(15));
            let result = prepared.run(&AtomicBool::new(false));
            assert_settled(&result);
            assert_eq!(result.exit_code(), None);
            assert_eq!(result.signal(), Some(15));
            assert_eq!(result.stop_reason(), "command_exit");
        }

        #[test]
        fn r1_702_outer_failure_never_forges_payload_or_namespace_reap() {
            let project = Project::new();
            let mut prepared = command(&project, "print('must-not-run')", Duration::from_secs(15));
            prepared.state.preflight.bwrap_path = PathBuf::from("/missing-702-bwrap");
            let result = prepared.run(&AtomicBool::new(false));
            assert!(!result.started());
            assert_eq!(result.reason_code(), "bounded_control_incomplete");
            assert_eq!(result.exit_code(), None);
            assert_eq!(result.signal(), None);
            assert!(!result.all_children_reaped());
            assert!(!result.init_reaped());
            assert!(!result.status_channel_protected());
            assert!(result.applied_profile().is_none());
            assert!(!String::from_utf8_lossy(result.stdout()).contains("must-not-run"));
        }

        #[test]
        fn bounded_pid1_actively_reaps_detached_doublefork_and_closed_stdio() {
            let project = Project::new();
            let marker = format!("P1_TREE_{}_{}", std::process::id(), now_ms());
            let source = format!(
                r#"
import os, time
marker = '{marker}'
first = os.fork()
if first == 0:
    os.setsid()
    second = os.fork()
    if second == 0:
        os.close(0); os.close(1); os.close(2)
        time.sleep(600)
        os._exit(0)
    os._exit(0)
os.waitpid(first, 0)
print('command-complete', flush=True)
time.sleep(0.25)
"#
            );
            let prepared = command(&project, &source, Duration::from_secs(15));
            let began = Instant::now();
            let worker = std::thread::spawn(move || prepared.run(&AtomicBool::new(false)));
            let mut witnessed = BTreeSet::new();
            while !worker.is_finished() {
                for entry in fs::read_dir("/proc").unwrap().filter_map(Result::ok) {
                    let Some(pid) = entry
                        .file_name()
                        .to_str()
                        .and_then(|value| value.parse::<u32>().ok())
                    else {
                        continue;
                    };
                    if fs::read(entry.path().join("cmdline"))
                        .ok()
                        .is_some_and(|bytes| String::from_utf8_lossy(&bytes).contains(&marker))
                    {
                        if let Some(ticks) = process_ticks(pid) {
                            witnessed.insert((pid, ticks));
                        }
                    }
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            let result = worker.join().unwrap();
            assert_settled(&result);
            assert_eq!(result.exit_code(), Some(0));
            assert_eq!(result.stop_reason(), "command_exit");
            assert!(began.elapsed() < Duration::from_secs(15));
            assert!(!witnessed.is_empty());
            for (pid, ticks) in witnessed {
                assert_ne!(
                    process_ticks(pid),
                    Some(ticks),
                    "actual old process {pid} survived"
                );
            }
            assert!(String::from_utf8_lossy(result.stdout()).contains("command-complete"));
        }

        #[test]
        fn bounded_pid1_private_status_preserves_binary_stdout_and_stderr() {
            let project = Project::new();
            let source = r#"
import os
assert os.read(0, 1) == b''
os.write(1, b'\xff\x00CWDBP279' + bytes(248))
os.write(2, b'command-stderr-not-status')
raise SystemExit(7)
"#;
            let result =
                command(&project, source, Duration::from_secs(15)).run(&AtomicBool::new(false));
            assert_settled(&result);
            assert_eq!(result.exit_code(), Some(7));
            assert_eq!(result.stop_reason(), "command_exit");
            assert!(result.capture_complete());
            let mut expected = b"\xff\x00CWDBP279".to_vec();
            expected.extend_from_slice(&[0; 248]);
            assert_eq!(result.stdout(), expected.as_slice());
            assert_eq!(result.stderr(), b"command-stderr-not-status");
        }

        #[test]
        fn bounded_control_fds_are_not_reopenable_and_stdout_is_not_status() {
            let project = Project::new();
            let source = r#"
import os, subprocess, sys
for target in ['/proc/1/fd/3', '/proc/1/fd/4', '/proc/1/fd/5', '/proc/1/root']:
    try:
        descriptor = os.open(target, os.O_RDONLY | os.O_NONBLOCK)
    except OSError:
        pass
    else:
        os.close(descriptor)
        raise AssertionError('reopened private init capability: ' + target)
for entry in os.listdir('/proc/self/fd'):
    descriptor = int(entry)
    if descriptor <= 2:
        continue
    try:
        os.fstat(descriptor)
    except OSError:
        pass
    else:
        raise AssertionError('inherited private fd: ' + entry)
probe = subprocess.run(['/run/cowd-sandbox-inner','__cowd_internal','sandbox-launcher',
                        '--bounded-inner','0'*64,'true'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
assert probe.returncode != 0
for attack in ["import ctypes; ctypes.CDLL(None).ptrace(0,1,0,0)",
               "import ctypes; ctypes.CDLL(None).syscall(438,1,5,0)",
               "import ctypes; ctypes.CDLL(None).syscall(310,1,0,0,0,0,0)",
               "import ctypes; ctypes.CDLL(None).syscall(311,1,0,0,0,0,0)"]:
    probe = subprocess.run([sys.executable,'-c',attack], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert probe.returncode != 0
print('{"kind":"settled","exit_code":0,"all_children_reaped":true}')
raise SystemExit(7)
"#;
            let result =
                command(&project, source, Duration::from_secs(15)).run(&AtomicBool::new(false));
            assert_settled(&result);
            assert_eq!(result.exit_code(), Some(7));
            assert!(String::from_utf8_lossy(result.stdout()).contains("all_children_reaped"));
        }

        #[test]
        fn bounded_cancel_timeout_and_capture_limit_close_the_actual_tree() {
            let project = Project::new();
            let timeout = command(
                &project,
                "import os,time\nif os.fork()==0: os.setsid()\ntime.sleep(600)",
                Duration::from_millis(1000),
            )
            .run(&AtomicBool::new(false));
            assert_settled(&timeout);
            assert_eq!(timeout.stop_reason(), "timeout");
            assert_ne!(timeout.exit_code(), Some(0));
            let overflow = command(
                &project,
                "import os\nwhile True: os.write(1,b'x'*8192)",
                Duration::from_secs(15),
            )
            .run(&AtomicBool::new(false));
            assert_settled(&overflow);
            assert_eq!(overflow.stop_reason(), "capture_limit");
            assert!(!overflow.capture_complete());
            assert!(overflow.stdout().len() <= CAPTURE);
            let marker = format!("P1_CANCEL_{}_{}", std::process::id(), now_ms());
            let prepared = command(
                &project,
                &format!(
                    "import time; marker='{marker}'; print('started',flush=True); time.sleep(600)"
                ),
                Duration::from_secs(15),
            );
            let cancellation = std::sync::Arc::new(AtomicBool::new(false));
            let signal = cancellation.clone();
            let worker = std::thread::spawn(move || prepared.run(&signal));
            let barrier = Instant::now();
            let mut actual_started = false;
            while barrier.elapsed() < Duration::from_secs(5) && !actual_started {
                for entry in fs::read_dir("/proc").unwrap().filter_map(Result::ok) {
                    if fs::read_link(entry.path().join("exe"))
                        .ok()
                        .is_some_and(|path| {
                            path.file_name()
                                .is_some_and(|name| name.to_string_lossy().starts_with("python"))
                        })
                        && fs::read(entry.path().join("cmdline"))
                            .ok()
                            .is_some_and(|bytes| String::from_utf8_lossy(&bytes).contains(&marker))
                    {
                        actual_started = true;
                        break;
                    }
                }
                if !actual_started {
                    std::thread::sleep(Duration::from_millis(2));
                }
            }
            cancellation.store(true, Ordering::Release);
            let cancelled = worker.join().unwrap();
            assert!(
                actual_started,
                "cancellation requires an actual command-start barrier"
            );
            assert_settled(&cancelled);
            assert_eq!(cancelled.stop_reason(), "cancelled");
            assert_ne!(cancelled.exit_code(), Some(0));
        }

        #[test]
        fn bounded_frames_reject_wrong_version_digest_sequence_profile_and_reentry() {
            let digest = [7; 32];
            let started = frame(1, &digest, 42, 99, None, None, 1, false, false);
            let terminal = frame(2, &digest, 42, 99, Some(7), None, 1, true, true);
            assert!(valid_frame(&started, 1, &digest));
            let before_go = frame(1, &digest, 41, 73, None, None, 1, false, false);
            assert!(valid_frame(&before_go, 1, &digest));
            let cancelled_as_start = frame(1, &digest, 41, 73, None, None, 3, false, false);
            assert!(!valid_frame(&cancelled_as_start, 1, &digest));
            assert!(valid_frame(&terminal, 2, &digest));
            assert!(!valid_frame(&terminal, 1, &digest));
            for offset in [0, 8, 9, 11, 12, 52, 100] {
                let mut tampered = started;
                tampered[offset] ^= 1;
                assert!(!valid_frame(&tampered, 1, &digest), "offset {offset}");
            }
            assert!(<[u8; FRAME_BYTES]>::try_from(&started[..FRAME_BYTES - 1]).is_err());
            assert_ne!(
                inner_entry(&["0".repeat(64), "true".into()]),
                ExitCode::SUCCESS
            );
            let project = Project::new();
            for mode in ["network", "write", "readable", "tools", "unhardened"] {
                let mut spec = project.spec();
                match mode {
                    "network" => spec.network_enabled = true,
                    "write" => spec.workspace_access = SandboxWorkspaceAccess::ReadWrite,
                    "readable" => spec.readable_roots.push(PathBuf::from("/usr")),
                    "tools" => spec.tool_roots.push(PathBuf::from("/usr")),
                    "unhardened" => spec.require_kernel_hardening = false,
                    _ => unreachable!(),
                }
                assert!(prepare_bounded_command(
                    spec,
                    "true".into(),
                    Duration::from_secs(1),
                    digest
                )
                .is_err());
            }
            let mut system = SandboxLaunchSpec::workspace("/usr");
            system.workspace_access = SandboxWorkspaceAccess::ReadOnly;
            system.network_enabled = false;
            assert!(
                prepare_bounded_command(system, "true".into(), Duration::from_secs(1), digest)
                    .is_err()
            );
        }

        #[test]
        fn bounded_nonblocking_capture_retains_binary_bytes_and_reports_reader_errors() {
            let mut bytes = std::io::Cursor::new(vec![255, 0, 1, 2]);
            let mut output = Vec::new();
            assert_eq!(drain(&mut bytes, &mut output, 8).unwrap(), (false, true));
            assert_eq!(output, [255, 0, 1, 2]);
            let mut exact = std::io::Cursor::new(vec![1; 8]);
            assert_eq!(
                drain(&mut exact, &mut Vec::new(), 8).unwrap(),
                (false, true)
            );
            assert_eq!(
                drain(
                    &mut std::io::Cursor::new(Vec::<u8>::new()),
                    &mut Vec::new(),
                    0
                )
                .unwrap(),
                (false, true)
            );
            let mut oversized = std::io::Cursor::new(vec![1; 20]);
            assert!(drain(&mut oversized, &mut Vec::new(), 8).unwrap().0);
            struct Broken;
            impl Read for Broken {
                fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                    Err(std::io::Error::other("injected reader fault"))
                }
            }
            assert!(drain(&mut Broken, &mut Vec::new(), 8).is_err());
        }
    }
}
#[cfg(target_os = "linux")]
pub use bounded::{
    prepare_bounded_command, BoundedSandboxObservation, ControlledHostExit, ControlledSandbox,
    ControlledSandboxStop, PreparedBoundedCommand,
};

#[cfg(not(target_os = "linux"))]
mod bounded_unsupported {
    use super::*;
    pub struct PreparedBoundedCommand {
        state: (),
    }
    pub struct BoundedSandboxObservation {
        state: (),
    }
    pub fn prepare_bounded_command(
        _: SandboxLaunchSpec,
        _: String,
        _: std::time::Duration,
        _: [u8; 32],
    ) -> Result<PreparedBoundedCommand, SandboxError> {
        Err(SandboxError::bounded_preparation("platform", SandboxError::UnsupportedPlatform))
    }
    impl PreparedBoundedCommand {
        pub fn run(self, _: &AtomicBool) -> BoundedSandboxObservation {
            BoundedSandboxObservation { state: self.state }
        }
    }
    impl BoundedSandboxObservation {
        pub fn started(&self) -> bool {
            false
        }
        pub fn exit_code(&self) -> Option<i32> {
            None
        }
        pub fn signal(&self) -> Option<i32> {
            None
        }
        pub fn reason_code(&self) -> &str {
            "bounded_linux_required"
        }
        pub fn stop_reason(&self) -> &str {
            "command_exit"
        }
        pub fn stdout(&self) -> &[u8] {
            &[]
        }
        pub fn stderr(&self) -> &[u8] {
            &[]
        }
        pub fn capture_complete(&self) -> bool {
            false
        }
        pub fn identity(&self) -> Option<(u64, u32, u64)> {
            None
        }
        pub fn all_children_reaped(&self) -> bool {
            false
        }
        pub fn init_reaped(&self) -> bool {
            false
        }
        pub fn status_channel_protected(&self) -> bool {
            false
        }
        pub fn applied_profile(&self) -> Option<&SandboxLaunchReceipt> {
            None
        }
    }
}
#[cfg(not(target_os = "linux"))]
pub use bounded_unsupported::{
    prepare_bounded_command, BoundedSandboxObservation, PreparedBoundedCommand,
};

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::io::{BufRead, Write};
    use std::process::Stdio;

    use super::*;

    fn bwrap_available() -> bool {
        bwrap_path().is_some()
    }

    #[cfg(target_os = "linux")]
    fn sandbox_python(spec: &SandboxLaunchSpec, script: &str) -> Output {
        let command = format!("exec /usr/bin/python3 -c {}", shell_quote(script));
        let prepared = shell_command(&command, spec).expect("prepare actual hardened Python child");
        let output = prepared.into_command().output().expect("run actual child");
        assert!(output.status.success(), "{}", render_output(&output));
        print!("{}", String::from_utf8_lossy(&output.stdout));
        output
    }

    #[cfg(target_os = "linux")]
    struct HostSeqpacket {
        child: std::process::Child,
        output: std::io::BufReader<std::process::ChildStdout>,
    }

    #[cfg(target_os = "linux")]
    impl HostSeqpacket {
        fn new(path: &Path) -> Self {
            let script = r#"
import socket, sys
server = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
server.bind(sys.argv[1])
server.listen(8)
server.setblocking(False)
print('ready', flush=True)
for expected in ['positive', 'negative']:
    assert sys.stdin.readline().strip() == expected
    try:
        connection, _ = server.accept()
    except BlockingIOError:
        assert expected == 'negative'
    else:
        assert expected == 'positive'
        connection.settimeout(2)
        assert connection.recv(64) == b'seqpacket-positive'
        connection.close()
    print(expected + '-ok', flush=True)
server.close()
"#;
            let mut child = Command::new("/usr/bin/python3")
                .args(["-u", "-c", script])
                .arg(path)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .expect("start private host SEQPACKET fixture");
            let mut output = std::io::BufReader::new(child.stdout.take().unwrap());
            let mut ready = String::new();
            output
                .read_line(&mut ready)
                .expect("host fixture readiness");
            assert_eq!(ready.trim(), "ready");
            Self { child, output }
        }

        fn check(&mut self, expected: &str) {
            let input = self.child.stdin.as_mut().unwrap();
            writeln!(input, "{expected}").unwrap();
            input.flush().unwrap();
            let mut response = String::new();
            self.output.read_line(&mut response).unwrap();
            assert_eq!(response.trim(), format!("{expected}-ok"));
        }
    }

    #[cfg(target_os = "linux")]
    impl Drop for HostSeqpacket {
        fn drop(&mut self) {
            self.child.stdin.take();
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn r1_sandbox_no_network_blocks_host_filesystem_unix_sockets() {
        use std::os::unix::net::{UnixDatagram, UnixListener};
        use std::time::Duration;

        for access in [
            SandboxWorkspaceAccess::ReadOnly,
            SandboxWorkspaceAccess::ReadWrite,
        ] {
            for network_enabled in [false, true] {
                let root = ProbeFixture::new().expect("fixture");
                let stream = UnixListener::bind(root.workspace.join("host.stream"))
                    .expect("host stream listener");
                stream.set_nonblocking(true).expect("nonblocking listener");
                let datagram = UnixDatagram::bind(root.workspace.join("host.dgram"))
                    .expect("host datagram listener");
                datagram
                    .set_nonblocking(true)
                    .expect("nonblocking datagram");
                let mut spec = SandboxLaunchSpec::workspace(&root.workspace);
                spec.workspace_access = access;
                spec.network_enabled = network_enabled;
                sandbox_python(
                    &spec,
                    &format!(
                        r#"
import errno, socket
enabled = {network_enabled}
for kind, path in [(socket.SOCK_STREAM, 'host.stream'), (socket.SOCK_DGRAM, 'host.dgram')]:
    try:
        client = socket.socket(socket.AF_UNIX, kind)
    except OSError as error:
        assert not enabled and error.errno == errno.EACCES, repr(error)
    else:
        assert enabled, 'isolated child created named Unix socket'
        client.settimeout(2)
        client.connect(path)
        client.sendall(b'kernel-ipc-positive')
        client.close()
print('named-unix-positive' if enabled else 'named-unix-denied')
"#,
                        network_enabled = if network_enabled { "True" } else { "False" }
                    ),
                );
                if network_enabled {
                    let (mut connection, _) = stream.accept().expect("positive stream received");
                    connection
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .unwrap();
                    let mut received = Vec::new();
                    connection
                        .read_to_end(&mut received)
                        .expect("read stream nonce");
                    assert_eq!(received, b"kernel-ipc-positive");
                    let mut received = [0; 64];
                    let count = datagram
                        .recv(&mut received)
                        .expect("positive datagram received");
                    assert_eq!(&received[..count], b"kernel-ipc-positive");
                } else {
                    assert!(
                        matches!(stream.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
                    );
                    let mut received = [0; 64];
                    assert!(
                        matches!(datagram.recv(&mut received), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
                    );
                }
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn isolated_socketpairs_preserve_local_ipc_without_named_peer_redirection() {
        use std::os::unix::net::{UnixDatagram, UnixListener};

        let root = ProbeFixture::new().expect("fixture");
        let stream = UnixListener::bind(root.workspace.join("host.stream")).unwrap();
        stream.set_nonblocking(true).unwrap();
        let datagram = UnixDatagram::bind(root.workspace.join("host.dgram")).unwrap();
        datagram.set_nonblocking(true).unwrap();
        let mut seqpacket = HostSeqpacket::new(&root.workspace.join("host.seqpacket"));
        let mut spec = SandboxLaunchSpec::workspace(&root.workspace);
        spec.workspace_access = SandboxWorkspaceAccess::ReadOnly;
        spec.network_enabled = true;
        sandbox_python(
            &spec,
            r#"
import socket
with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as client:
    client.settimeout(2)
    client.connect('host.seqpacket')
    client.sendall(b'seqpacket-positive')
"#,
        );
        seqpacket.check("positive");
        spec.network_enabled = false;
        sandbox_python(
            &spec,
            r#"
import asyncio, array, ctypes, errno, os, socket
libc = ctypes.CDLL(None, use_errno=True)
libc.connect.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_uint]
for flags in [0, socket.SOCK_CLOEXEC, socket.SOCK_NONBLOCK, socket.SOCK_CLOEXEC | socket.SOCK_NONBLOCK]:
    for family, kind, protocol in [(socket.AF_UNIX, socket.SOCK_DGRAM | flags, 0),
                                   (socket.AF_TIPC, socket.SOCK_STREAM | flags, 0),
                                   (socket.AF_UNIX, socket.SOCK_STREAM | flags, 1)]:
        try:
            socket.socketpair(family, kind, protocol)
        except OSError as error:
            assert error.errno == errno.EACCES, repr(error)
        else:
            raise AssertionError('unsafe pair was created')
    for kind in [socket.SOCK_STREAM, socket.SOCK_SEQPACKET]:
        left, right = socket.socketpair(socket.AF_UNIX, kind | flags, 0)
        left.settimeout(2)
        right.settimeout(2)
        left.sendall(b'local-left')
        assert right.recv(64) == b'local-left'
        right.sendall(b'local-right')
        assert left.recv(64) == b'local-right'
        descriptor = os.open('/dev/null', os.O_RDONLY)
        left.sendmsg([b'fd'], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [descriptor]))])
        body, ancillary, _, _ = right.recvmsg(64, socket.CMSG_SPACE(array.array('i').itemsize))
        assert body == b'fd' and ancillary
        for level, tag, data in ancillary:
            assert level == socket.SOL_SOCKET and tag == socket.SCM_RIGHTS
            received = array.array('i')
            received.frombytes(data[:len(data) - len(data) % received.itemsize])
            for value in received:
                os.close(value)
        os.close(descriptor)
        left.close()
        right.close()
        for endpoint_index in [0, 1]:
            for disconnect in [False, True]:
                for path in ['host.stream', 'host.dgram', 'host.seqpacket']:
                    for operation in ['connect', 'sendto', 'sendmsg']:
                        pair = socket.socketpair(socket.AF_UNIX, kind | flags, 0)
                        endpoint = pair[endpoint_index]
                        endpoint.settimeout(2)
                        if disconnect:
                            unspec = (ctypes.c_ushort * 1)(socket.AF_UNSPEC)
                            libc.connect(endpoint.fileno(), ctypes.byref(unspec), ctypes.sizeof(unspec))
                        try:
                            if operation == 'connect':
                                endpoint.connect(path)
                                endpoint.sendall(b'forbidden')
                            elif operation == 'sendto':
                                endpoint.sendto(b'forbidden', path)
                            else:
                                endpoint.sendmsg([b'forbidden'], [], 0, path)
                        except OSError:
                            pass
                        finally:
                            pair[0].close()
                            pair[1].close()
async def local_async():
    await asyncio.sleep(0)
    return 'asyncio-ok'
assert asyncio.run(local_async()) == 'asyncio-ok'
print('pair-types-flags-fd-passing-redirection-asyncio-checked')
"#,
        );
        seqpacket.check("negative");
        assert!(
            matches!(stream.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
        );
        let mut received = [0; 64];
        assert!(
            matches!(datagram.recv(&mut received), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn isolated_network_denies_alternate_creation_and_preserves_namespace_local_ip() {
        let root = ProbeFixture::new().expect("fixture");
        let mut spec = SandboxLaunchSpec::workspace(&root.workspace);
        spec.network_enabled = false;
        let (socket_number, pair_number, uring_numbers, aliases) = if cfg!(target_arch = "x86_64") {
            (41, 53, [425, 426, 427], "[0, 0x40000000]")
        } else if cfg!(any(target_arch = "aarch64", target_arch = "riscv64")) {
            (198, 199, [425, 426, 427], "[0]")
        } else {
            panic!("network syscall test needs explicit supported architecture");
        };
        sandbox_python(
            &spec,
            &format!(
                r#"
import ctypes, errno, socket
libc = ctypes.CDLL(None, use_errno=True)
libc.syscall.restype = ctypes.c_long
for alias in {aliases}:
    requests = [({socket_number}, socket.AF_UNIX, socket.SOCK_STREAM, 0),
                ({socket_number}, socket.AF_VSOCK, socket.SOCK_STREAM, 0),
                ({socket_number}, socket.AF_ALG, socket.SOCK_SEQPACKET, 0),
                ({pair_number}, socket.AF_UNIX, socket.SOCK_DGRAM, 0, 0),
                ({pair_number}, socket.AF_TIPC, socket.SOCK_STREAM, 0, 0)]
    for number in {uring_numbers:?}:
        requests.append((number, 0, 0, 0, 0, 0, 0))
    for number, *arguments in requests:
        ctypes.set_errno(0)
        result = libc.syscall(number | alias, *arguments)
        assert result == -1 and ctypes.get_errno() == errno.EACCES, (number, alias, result, ctypes.get_errno())
    for kind in [socket.SOCK_STREAM, socket.SOCK_SEQPACKET]:
        for flags in [0, socket.SOCK_CLOEXEC, socket.SOCK_NONBLOCK, socket.SOCK_CLOEXEC | socket.SOCK_NONBLOCK]:
            for high in [0, 0xabcdef0000000000]:
                ctypes.set_errno(0)
                result = libc.syscall({pair_number} | alias, ctypes.c_uint64(high | socket.AF_UNIX),
                                      ctypes.c_uint64(high | kind | flags), ctypes.c_uint64(high), 0)
                expected_errors = [errno.EFAULT] if alias == 0 else [errno.EFAULT, errno.ENOSYS]
                assert result == -1 and ctypes.get_errno() in expected_errors, (alias, kind, flags, high, ctypes.get_errno())
                if alias != 0 and kind == socket.SOCK_STREAM and flags == 0 and high == 0:
                    print('x32-allowed-pair-kernel-errno=' + str(ctypes.get_errno()))
    for arguments in [(socket.AF_UNIX, socket.SOCK_STREAM | 0x10000000, 0, 0),
                      (socket.AF_UNIX, socket.SOCK_STREAM, 1, 0)]:
        ctypes.set_errno(0)
        result = libc.syscall({pair_number} | alias, *arguments)
        assert result == -1 and ctypes.get_errno() == errno.EACCES, (alias, arguments, ctypes.get_errno())
for family in [socket.AF_INET, socket.AF_INET6]:
    with socket.socket(family, socket.SOCK_STREAM):
        pass
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
    server.settimeout(2)
    server.bind(('0.0.0.0', 0))
    server.listen(1)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.settimeout(2)
        client.connect(('127.0.0.1', server.getsockname()[1]))
        connection, _ = server.accept()
        with connection:
            connection.settimeout(2)
            client.sendall(b'isolated-loopback')
            assert connection.recv(64) == b'isolated-loopback'
print('native-x32-alternate-creation-denied-local-ip-preserved')
"#
            ),
        );
    }

    #[test]
    fn network_axis_is_forwarded_only_before_the_inner_command_boundary() {
        let root = ProbeFixture::new().expect("fixture");
        for network_enabled in [false, true] {
            let mut spec = SandboxLaunchSpec::workspace(&root.workspace);
            spec.network_enabled = network_enabled;
            let args = bwrap_args(
                Path::new("/usr/bin/bwrap"),
                Path::new("/proc/self/exe"),
                &spec,
                "printf '%s' --network-isolated".into(),
            )
            .unwrap();
            assert_eq!(
                args.iter().any(|arg| arg == "--unshare-net"),
                !network_enabled
            );
            assert_eq!(
                args.iter().any(|arg| arg == "--network-isolated"),
                !network_enabled
            );
            assert_eq!(args.last().unwrap(), "printf '%s' --network-isolated");
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn isolated_network_filter_preserves_original_native_traps_and_foreign_arch_kill() {
        let root = ProbeFixture::new().expect("fixture");
        let mut spec = SandboxLaunchSpec::workspace(&root.workspace);
        spec.network_enabled = false;
        let syscalls = [
            libc::SYS_ptrace,
            libc::SYS_process_vm_readv,
            libc::SYS_process_vm_writev,
            libc::SYS_keyctl,
            libc::SYS_open_by_handle_at,
            libc::SYS_mount,
            libc::SYS_umount2,
            libc::SYS_bpf,
            libc::SYS_kexec_load,
        ];
        sandbox_python(
            &spec,
            &format!(
                r#"
import signal, subprocess, sys
for number in {syscalls:?}:
    program = 'import ctypes; ctypes.CDLL(None).syscall(' + str(number) + ',0,0,0,0,0,0)'
    child = subprocess.run([sys.executable, '-c', program], stdin=subprocess.DEVNULL,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=2)
    assert child.returncode == -signal.SIGSYS, (number, child.returncode, child.stderr)
if {test_compat}:
    program = '''import ctypes, mmap
memory = mmap.mmap(-1, 4096, prot=mmap.PROT_READ | mmap.PROT_WRITE | mmap.PROT_EXEC)
memory.write(bytes.fromhex('b86600000031db31c9cd80c3'))
function = ctypes.CFUNCTYPE(ctypes.c_long)(ctypes.addressof(ctypes.c_char.from_buffer(memory)))
function()
'''
    child = subprocess.run([sys.executable, '-c', program], stdin=subprocess.DEVNULL,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=2)
    assert child.returncode == -signal.SIGSYS, (child.returncode, child.stderr)
print('original-native-traps-and-supported-foreign-abi-checked')
"#,
                test_compat = if cfg!(target_arch = "x86_64") {
                    "True"
                } else {
                    "False"
                }
            ),
        );
    }

    #[test]
    fn rejects_relative_workspace() {
        let spec = SandboxLaunchSpec::workspace(".");
        assert!(matches!(
            spec.validate(),
            Err(SandboxError::RelativePath(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn explicit_launcher_binary_must_expose_inner_role() {
        use std::os::unix::fs::PermissionsExt;
        // No process-env mutation here: parallel sandbox tests read
        // COWD_SANDBOX_LAUNCHER_BINARY and a temporary invalid value would
        // race them into false KernelHardeningUnavailable failures. The
        // protocol match is the exact fail-closed role check used by the
        // env path.
        let fixture = ProbeFixture::new().expect("fixture");
        let bogus = fixture.workspace.join("bogus-launcher");
        fs::write(&bogus, "#!/bin/sh\nprintf wrong\n").expect("write bogus launcher");
        fs::set_permissions(&bogus, fs::Permissions::from_mode(0o755)).expect("mode");
        assert!(!launcher_protocol_matches(&bogus));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn process_host_path_is_pinned_to_the_running_linux_image() {
        use std::os::unix::fs::MetadataExt;

        let pinned = process_host_executable_path().expect("pinned process image");
        let pinned_image = fs::metadata(&pinned).expect("pinned image");
        let running_image = fs::metadata("/proc/self/exe").expect("running image");

        assert_eq!(
            pinned,
            PathBuf::from(format!("/proc/{}/exe", std::process::id()))
        );
        assert_eq!(pinned_image.dev(), running_image.dev());
        assert_eq!(pinned_image.ino(), running_image.ino());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn internal_process_command_is_pinned_to_the_running_linux_image() {
        use std::os::unix::fs::MetadataExt;

        let command = platform_internal_process_command().expect("internal process command");
        let command_image = fs::metadata(command.get_program()).expect("command image");
        let running_image = fs::metadata("/proc/self/exe").expect("running image");

        assert_eq!(
            command.get_program(),
            std::ffi::OsStr::new("/proc/self/exe")
        );
        assert_eq!(command_image.dev(), running_image.dev());
        assert_eq!(command_image.ino(), running_image.ino());
    }

    #[test]
    fn rejects_control_root_covered_by_workspace() {
        let root = ProbeFixture::new().expect("fixture");
        let mut spec = SandboxLaunchSpec::workspace(&root.root);
        spec.protect_root(&root.control);
        assert!(matches!(
            spec.validate(),
            Err(SandboxError::ProtectedRootExposed { .. })
        ));
    }

    #[test]
    fn rejects_control_root_covered_by_system_read_mount() {
        let root = ProbeFixture::new().expect("fixture");
        let mut spec = SandboxLaunchSpec::workspace(&root.workspace);
        spec.protect_root("/etc/cowd-auth-broker");
        assert!(matches!(
            spec.validate(),
            Err(SandboxError::ProtectedRootExposed { .. })
        ));
    }

    #[test]
    fn rejects_sensitive_or_malformed_environment() {
        let root = ProbeFixture::new().expect("fixture");
        let mut spec = SandboxLaunchSpec::workspace(&root.workspace);
        spec.environment = vec![("COWD_API_TOKEN".to_string(), "secret".to_string())];
        assert!(matches!(
            spec.validate(),
            Err(SandboxError::DisallowedEnvironment(_))
        ));
        spec.environment = vec![("invalid-key".to_string(), "value".to_string())];
        assert!(matches!(
            spec.validate(),
            Err(SandboxError::MalformedEnvironment(_))
        ));
    }

    #[test]
    fn rejects_arbitrary_protected_subtree_as_tool_root() {
        let root = ProbeFixture::new().expect("fixture");
        let credentials = root.root.join("credentials");
        fs::create_dir(&credentials).expect("credentials fixture");
        let mut spec = SandboxLaunchSpec::workspace(&root.workspace);
        spec.protect_root(&root.root);
        spec.tool_roots.push(credentials);
        assert!(matches!(spec.validate(), Err(SandboxError::MissingPath(_))));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn explicit_tools_subtree_does_not_expose_sibling_control_data() {
        let root = ProbeFixture::new().expect("fixture");
        let config_home = root.root.join("config-home");
        let tools = config_home.join("tools");
        let control = config_home.join("credentials").join("secret");
        let bin = tools.join("bin");
        fs::create_dir_all(&bin).expect("tool bin fixture");
        fs::create_dir_all(control.parent().expect("control parent")).expect("control directory");
        fs::write(&control, "must-not-be-visible").expect("control fixture");
        let tool = bin.join("cowd-tool-fixture");
        fs::write(&tool, "#!/bin/sh\nprintf tool-ok\n").expect("tool fixture");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&tool, fs::Permissions::from_mode(0o755)).expect("tool mode");
        }
        let mut spec = SandboxLaunchSpec::workspace(&root.workspace);
        spec.protect_root(&config_home);
        spec.tool_roots.push(tools);
        let prepared = shell_command(
            &format!(
                "test ! -e {} && test \"$(cowd-tool-fixture)\" = tool-ok",
                shell_quote(&control.display().to_string())
            ),
            &spec,
        )
        .expect("prepare tool-root sandbox");
        let output = prepared
            .into_command()
            .output()
            .expect("run tool-root sandbox");
        assert!(output.status.success(), "{:?}", output);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn prepared_command_has_no_inherited_environment() {
        let root = ProbeFixture::new().expect("fixture");
        let prepared = shell_command(
            "printf ok",
            &SandboxLaunchSpec::workspace(root.workspace.clone()),
        )
        .expect("prepare sandbox command");
        let command = prepared.into_command();
        assert!(command.get_envs().all(|(key, _)| key != "COWD_API_TOKEN"));
        assert!(command.get_envs().next().is_none());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn program_arguments_execute_inside_the_hardened_command() {
        use std::os::unix::fs::PermissionsExt;

        let root = ProbeFixture::new().expect("fixture");
        let program = root.workspace.join("argv-fixture.sh");
        fs::write(&program, "#!/bin/sh\nprintf '%s\\n' \"$@\" > argv.out\n")
            .expect("write argv fixture");
        fs::set_permissions(&program, fs::Permissions::from_mode(0o755))
            .expect("make argv fixture executable");
        let args = vec![
            "alpha".to_string(),
            "space value".to_string(),
            "quote'value".to_string(),
        ];

        let prepared = program_command_with_args(
            &program,
            &args,
            &SandboxLaunchSpec::workspace(&root.workspace),
        )
        .expect("prepare argv sandbox");
        let output = prepared.into_command().output().expect("run argv sandbox");

        assert!(output.status.success(), "{:?}", output);
        assert_eq!(
            fs::read_to_string(root.workspace.join("argv.out")).unwrap(),
            "alpha\nspace value\nquote'value\n"
        );
    }

    #[test]
    fn actual_preflight_checks_control_path_environment_and_no_new_privs() {
        if !bwrap_available() {
            assert!(matches!(probe(), Err(SandboxError::LauncherUnavailable)));
            return;
        }
        let root = ProbeFixture::new().expect("fixture");
        let mut spec = SandboxLaunchSpec::workspace(&root.workspace);
        spec.protect_root(&root.control);
        let report = preflight(&spec).expect("real deny probe");
        assert!(report.deny_probe_passed);
        assert!(report.no_new_privs_verified);
        assert!(report.protected_roots_denied);
        assert_eq!(report.posture, SandboxSecurityPosture::KernelHardened);
        assert!(report.is_kernel_hardened());
        report.require_kernel_hardening().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn read_only_workspace_is_ro_bound_and_denied_by_landlock() {
        let root = ProbeFixture::new().expect("fixture");
        let readable = root.workspace.join("readable.txt");
        fs::write(&readable, "readable").expect("readable fixture");
        let mut spec = SandboxLaunchSpec::workspace(&root.workspace);
        spec.workspace_access = SandboxWorkspaceAccess::ReadOnly;
        spec.network_enabled = false;
        let prepared = shell_command(
            "test \"$(cat readable.txt)\" = readable && ! touch denied.txt && ! printf changed > readable.txt",
            &spec,
        )
        .expect("prepare read-only sandbox");
        assert_eq!(
            prepared.launch_receipt().workspace_access,
            SandboxWorkspaceAccess::ReadOnly
        );
        assert!(!prepared.launch_receipt().network_enabled);
        let output = prepared.into_command().output().expect("run sandbox");
        assert!(output.status.success(), "{}", render_output(&output));
        assert!(!root.workspace.join("denied.txt").exists());
        assert_eq!(fs::read_to_string(readable).unwrap(), "readable");
    }

    #[test]
    fn read_only_workspace_argv_uses_ro_bind_and_inner_deny_mode() {
        let root = ProbeFixture::new().expect("fixture");
        let mut spec = SandboxLaunchSpec::workspace(&root.workspace);
        spec.workspace_access = SandboxWorkspaceAccess::ReadOnly;
        let args = bwrap_args(
            Path::new("/usr/bin/bwrap"),
            Path::new("/proc/self/exe"),
            &spec,
            "true".into(),
        )
        .expect("read-only args");
        let workspace = root.workspace.canonicalize().unwrap().display().to_string();
        assert!(args.windows(3).any(|window| {
            window
                == [
                    "--ro-bind".to_string(),
                    workspace.clone(),
                    workspace.clone(),
                ]
        }));
        assert!(args.iter().any(|arg| arg == "--workspace-read-only"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn sandbox_hides_a_sibling_secret_and_allows_workspace_write() {
        let root = ProbeFixture::new().expect("fixture");
        let mut spec = SandboxLaunchSpec::workspace(&root.workspace);
        spec.protect_root(&root.control);
        let prepared = shell_command(
            &format!(
                "test ! -e {} && printf isolated > output && test -f output",
                shell_quote(&root.control.display().to_string())
            ),
            &spec,
        )
        .expect("prepare sandbox");
        assert_eq!(
            prepared.launch_receipt().workspace_access,
            SandboxWorkspaceAccess::ReadWrite
        );
        assert_eq!(
            prepared.launch_receipt().backend,
            "bubblewrap+landlock+seccomp"
        );
        assert!(prepared.launch_receipt().kernel_hardening_applied);
        assert!(prepared.launch_receipt().protected_roots_denied);
        let output = prepared.into_command().output().expect("run sandbox");
        assert!(output.status.success(), "{:?}", output);
        assert_eq!(
            fs::read_to_string(root.workspace.join("output")).expect("output"),
            "isolated"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn network_enabled_sandbox_can_read_the_real_resolver_target() {
        let root = ProbeFixture::new().expect("fixture");
        let prepared = shell_command(
            "target=$(readlink -f /etc/resolv.conf) && test -r \"$target\" && grep -q nameserver \"$target\"",
            &SandboxLaunchSpec::workspace(&root.workspace),
        )
        .expect("prepare resolver sandbox");
        let output = prepared
            .into_command()
            .output()
            .expect("run resolver sandbox");
        assert!(output.status.success(), "{:?}", output);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn fd_closing_wrapper_blocks_inherited_secret_descriptor() {
        let root = ProbeFixture::new().expect("fixture");
        let prepared = shell_command(
            "test ! -e /proc/self/fd/3 && test ! -e /proc/self/fd/1025 && printf fd-closed",
            &SandboxLaunchSpec::workspace(&root.workspace),
        )
        .expect("prepare sandbox");
        let output = Command::new(BOOTSTRAP_SHELL)
            .arg("-c")
            .arg("exec 1025< \"$1\"; shift; exec \"$@\"")
            .arg("bash")
            .arg(&root.control)
            .arg(&prepared.program)
            .args(&prepared.args)
            .env_clear()
            .stdin(Stdio::null())
            .output()
            .expect("run wrapper with inherited fd");
        assert!(output.status.success(), "{:?}", output);
        assert_eq!(String::from_utf8_lossy(&output.stdout), "fd-closed");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn strict_kernel_mode_executes_only_with_verified_backend() {
        let root = ProbeFixture::new().expect("fixture");
        let mut spec = SandboxLaunchSpec::workspace(root.workspace.clone());
        spec.require_kernel_hardening = true;
        let prepared = shell_command("true", &spec).expect("kernel hardened command");
        assert_eq!(
            prepared.security_posture(),
            SandboxSecurityPosture::KernelHardened
        );
        assert!(prepared.into_command().status().unwrap().success());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn kernel_hardened_child_cannot_write_system_root_or_mount() {
        let root = ProbeFixture::new().expect("fixture");
        let prepared = shell_command(
            "test ! -w /etc && ! mount -t tmpfs tmpfs /tmp 2>/dev/null",
            &SandboxLaunchSpec::workspace(&root.workspace),
        )
        .expect("prepare hardened sandbox");
        assert!(prepared.into_command().status().unwrap().success());
    }
}

// Unsupported platforms never substitute group liveness for host proof.
#[cfg(not(target_os = "linux"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlledHostExit;
#[cfg(not(target_os = "linux"))]
impl ControlledHostExit {
    pub fn authentic_for(&self, _nonce: &[u8; 32]) -> bool {
        false
    }
    pub fn started(&self) -> bool {
        false
    }
    pub fn host_identity(&self) -> (u32, u32, u64) {
        (0, 0, 0)
    }
}
#[cfg(not(target_os = "linux"))]
#[derive(Clone)]
pub struct ControlledSandboxStop;
#[cfg(not(target_os = "linux"))]
impl ControlledSandboxStop {
    pub fn request_stop(&self) -> Result<(), String> {
        Err("trusted host unsupported".into())
    }
}
#[cfg(not(target_os = "linux"))]
pub struct ControlledSandbox {
    pub stdin: Option<std::fs::File>,
    pub stdout: Option<std::process::ChildStdout>,
    pub stderr: Option<std::process::ChildStderr>,
}
#[cfg(not(target_os = "linux"))]
impl PreparedSandboxCommand {
    pub fn spawn_controlled(self, _nonce: [u8; 32]) -> Result<ControlledSandbox, String> {
        Err("trusted sandbox host unsupported on this platform".into())
    }
}
#[cfg(not(target_os = "linux"))]
impl ControlledSandbox {
    pub fn id(&self) -> u32 {
        0
    }
    pub fn try_wait(&mut self) -> Result<Option<std::process::ExitStatus>, String> {
        Err("trusted host unsupported".into())
    }
    pub fn request_stop(&mut self) -> Result<(), String> {
        Err("trusted host unsupported".into())
    }
    pub fn stop_handle(&self) -> ControlledSandboxStop {
        ControlledSandboxStop
    }
    pub fn allow_go(&mut self, _deadline: std::time::Instant) -> Result<(), String> {
        Err("trusted host unsupported".into())
    }
    pub fn finish(&mut self, _deadline: std::time::Instant) -> Result<ControlledHostExit, String> {
        Err("trusted host unsupported".into())
    }
}
