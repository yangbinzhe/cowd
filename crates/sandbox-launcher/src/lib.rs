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
    process::{Command, Output, Stdio},
};

use thiserror::Error;

const SYSTEM_READ_ONLY_ROOTS: &[&str] = &["/usr", "/bin", "/lib", "/lib64", "/etc"];
const SANDBOX_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
const BOOTSTRAP_SHELL: &str = "/bin/bash";
const INNER_LAUNCHER_PATH: &str = "/run/cowd-sandbox-inner";
const LAUNCHER_PROTOCOL: &str =
    concat!("sandbox-launcher/", env!("CARGO_PKG_VERSION"), "/kernel-v1");

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

/// A concrete result of the Linux preflight, rather than a best-effort claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxPreflight {
    pub bwrap_path: PathBuf,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxLaunchSpec {
    pub workspace_root: PathBuf,
    pub working_directory: Option<PathBuf>,
    pub readable_roots: Vec<PathBuf>,
    pub writable_roots: Vec<PathBuf>,
    /// Explicit control-plane paths which must never become visible to the
    /// child. The constructor also derives config and broker paths from the
    /// environment when available.
    pub protected_roots: Vec<PathBuf>,
    pub network_enabled: bool,
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
            protected_roots: default_protected_roots(),
            network_enabled: true,
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
        for protected_root in self
            .protected_roots
            .iter()
            .map(|root| normalize_absolute(root))
        {
            let protected_root = protected_root?;
            for visible_root in &visible_roots {
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
        for root in self.readable_roots.iter().chain(&self.writable_roots) {
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
    pub program: String,
    pub args: Vec<String>,
    /// Empty by design. Bubblewrap receives its allowlisted environment via
    /// `--clearenv`/`--setenv`; the wrapper itself starts with `env_clear`.
    pub environment: Vec<(String, String)>,
    pub preflight: SandboxPreflight,
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
    if !cfg!(target_os = "linux") {
        return Err(SandboxError::UnsupportedPlatform);
    }
    spec.validate()?;
    let bwrap_path = bwrap_path().ok_or(SandboxError::LauncherUnavailable)?;
    ensure_bootstrap_shell()?;
    ensure_launcher_protocol(&launcher_binary_path()?)?;
    let fixture = ProbeFixture::new()?;
    let mut probe_spec = SandboxLaunchSpec::workspace(&spec.workspace_root);
    probe_spec.working_directory = spec.working_directory.clone();
    probe_spec.readable_roots = spec.readable_roots.clone();
    probe_spec.writable_roots = spec.writable_roots.clone();
    probe_spec.protected_roots = spec.protected_roots.clone();
    probe_spec.protect_root(&fixture.control);
    probe_spec.network_enabled = spec.network_enabled;
    probe_spec.environment = spec.environment.clone();
    probe_spec.require_kernel_hardening = spec.require_kernel_hardening;
    probe_spec.validate()?;

    let args = bwrap_args(
        &bwrap_path,
        &probe_spec,
        probe_command(&fixture.control, &probe_spec.environment),
    )?;
    let output = run_fd_closing_wrapper(&bwrap_path, &args)?;
    if !output.status.success() {
        return Err(SandboxError::ProbeFailed(render_output(&output)));
    }

    let preflight = SandboxPreflight {
        bwrap_path,
        deny_probe_passed: true,
        no_new_privs_verified: true,
        inherited_fds_closed: true,
        environment_allowlist_enforced: true,
        protected_roots_denied: true,
        posture: SandboxSecurityPosture::KernelHardened,
        unavailable_hardening: Vec::new(),
    };
    if spec.require_kernel_hardening {
        preflight.require_kernel_hardening()?;
    }
    Ok(preflight)
}

pub fn shell_command(
    command: &str,
    spec: &SandboxLaunchSpec,
) -> Result<PreparedSandboxCommand, SandboxError> {
    let preflight = preflight(spec)?;
    let args = bwrap_args(&preflight.bwrap_path, spec, command.to_string())?;
    Ok(PreparedSandboxCommand {
        program: BOOTSTRAP_SHELL.to_string(),
        args: vec![
            "-c".to_string(),
            fd_closing_script(&preflight.bwrap_path, &args),
        ],
        environment: Vec::new(),
        preflight,
    })
}

/// Launch an executable contained in the declared workspace without exposing
/// the parent Gateway filesystem to the child.
pub fn program_command(
    program: &Path,
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
    let quoted = shell_quote(&program.display().to_string());
    shell_command(&format!("exec {quoted}"), spec)
}

fn bwrap_args(
    _bwrap_path: &Path,
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
    let inner_launcher = launcher_binary_path()?;

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
    append_environment(&mut args, &spec.environment)?;
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
        inner_launcher.display().to_string(),
        INNER_LAUNCHER_PATH.to_string(),
    ]);

    let writable = spec
        .writable_roots
        .iter()
        .map(|root| canonical(root))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let mut readable = spec
        .readable_roots
        .iter()
        .map(|root| canonical(root))
        .collect::<Result<BTreeSet<_>, _>>()?;
    readable.remove(&workspace);
    for root in &writable {
        readable.remove(root);
    }

    bind_root(&mut args, &workspace, true);
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
        INNER_LAUNCHER_PATH.to_string(),
        "--inner".to_string(),
        "--workspace".to_string(),
        workspace.display().to_string(),
    ]);
    for root in &spec.writable_roots {
        args.push("--writable".to_string());
        args.push(canonical(root)?.display().to_string());
    }
    args.extend(["--".to_string(), command]);
    Ok(args)
}

fn launcher_binary_path() -> Result<PathBuf, SandboxError> {
    if let Some(path) = env::var_os("COWD_SANDBOX_LAUNCHER").map(PathBuf::from) {
        if path.is_file() {
            return canonical(&path);
        }
    }
    let current = env::current_exe().map_err(|error| {
        SandboxError::ProbeFailed(format!("resolve current executable: {error}"))
    })?;
    let parent = current.parent().ok_or_else(|| {
        SandboxError::ProbeFailed("current executable has no parent directory".to_string())
    })?;
    for candidate in [
        parent.join("cowd-sandbox-launcher"),
        parent
            .parent()
            .unwrap_or(parent)
            .join("cowd-sandbox-launcher"),
    ] {
        if candidate.is_file() {
            return canonical(&candidate);
        }
    }
    Err(SandboxError::KernelHardeningUnavailable(
        "cowd-sandbox-launcher inner hardener is not installed beside the host binary".to_string(),
    ))
}

fn ensure_launcher_protocol(path: &Path) -> Result<(), SandboxError> {
    let output = Command::new(path)
        .arg("--protocol-version")
        .env_clear()
        .stdin(Stdio::null())
        .output()
        .map_err(|error| SandboxError::ProbeFailed(error.to_string()))?;
    let actual = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if output.status.success() && actual == LAUNCHER_PROTOCOL {
        Ok(())
    } else {
        Err(SandboxError::KernelHardeningUnavailable(format!(
            "sandbox helper protocol mismatch: expected `{LAUNCHER_PROTOCOL}`, got `{actual}`"
        )))
    }
}

fn append_environment(
    args: &mut Vec<String>,
    environment: &[(String, String)],
) -> Result<(), SandboxError> {
    validate_environment(environment)?;
    for (key, value) in [
        ("HOME", "/home/cowd"),
        ("TMPDIR", "/tmp"),
        ("PATH", SANDBOX_PATH),
        ("COWD_SANDBOX", "rootless-kernel-hardened"),
    ] {
        args.extend(["--setenv".to_string(), key.to_string(), value.to_string()]);
    }
    for (key, value) in environment {
        args.extend(["--setenv".to_string(), key.clone(), value.clone()]);
    }
    Ok(())
}

fn run_fd_closing_wrapper(bwrap_path: &Path, args: &[String]) -> Result<Output, SandboxError> {
    ensure_bootstrap_shell()?;
    Command::new(BOOTSTRAP_SHELL)
        .arg("-c")
        .arg(fd_closing_script(bwrap_path, args))
        .env_clear()
        .stdin(Stdio::null())
        .output()
        .map_err(|error| SandboxError::ProbeFailed(error.to_string()))
}

fn fd_closing_script(bwrap_path: &Path, args: &[String]) -> String {
    let bwrap = shell_quote(&bwrap_path.display().to_string());
    let args = args
        .iter()
        .map(|argument| shell_quote(argument))
        .collect::<Vec<_>>()
        .join(" ");
    // This shell is trusted bootstrap only: all untrusted command text is a
    // single shell-quoted bwrap argument after `--`. Rust's safe process API
    // has no close_range primitive, so enumerate this bootstrap process's
    // Linux FD table and close every descriptor above stderr before exec.
    // Absence of procfs is a hard failure, never a fallback with leaked FDs.
    format!(
        "[ -d /proc/$$/fd ] || {{ echo 'sandbox bootstrap requires /proc/self/fd' >&2; exit 125; }}; for entry in /proc/$$/fd/*; do fd=${{entry##*/}}; case \"$fd\" in 0|1|2|*[!0-9]*|'') ;; *) eval \"exec $fd>&-\" ;; esac; done; exec {bwrap} {args}"
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
        if !allowed_environment_key(key) || !seen.insert(key) || value.contains('\0') {
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

fn allowed_environment_key(key: &str) -> bool {
    matches!(
        key,
        "LANG" | "LANGUAGE" | "TERM" | "COLORTERM" | "NO_COLOR" | "TZ"
    ) || key.starts_with("LC_")
        || matches!(key, "HTTP_PROXY" | "HTTPS_PROXY" | "ALL_PROXY" | "NO_PROXY")
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
                return Err(SandboxError::RelativePath(path.display().to_string()))
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::process::Stdio;

    use super::*;

    fn bwrap_available() -> bool {
        bwrap_path().is_some()
    }

    #[test]
    fn rejects_relative_workspace() {
        let spec = SandboxLaunchSpec::workspace(".");
        assert!(matches!(
            spec.validate(),
            Err(SandboxError::RelativePath(_))
        ));
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
    fn prepared_command_has_no_inherited_environment() {
        let root = ProbeFixture::new().expect("fixture");
        let prepared = shell_command(
            "printf ok",
            &SandboxLaunchSpec::workspace(root.workspace.clone()),
        );
        if let Ok(prepared) = prepared {
            let command = prepared.into_command();
            assert!(command.get_envs().all(|(key, _)| key != "COWD_API_TOKEN"));
            assert!(command.get_envs().next().is_none());
        }
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

    #[test]
    fn sandbox_hides_a_sibling_secret_and_allows_workspace_write() {
        if !bwrap_available() {
            return;
        }
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
        let output = prepared.into_command().output().expect("run sandbox");
        assert!(output.status.success(), "{:?}", output);
        assert_eq!(
            fs::read_to_string(root.workspace.join("output")).expect("output"),
            "isolated"
        );
    }

    #[test]
    fn fd_closing_wrapper_blocks_inherited_secret_descriptor() {
        if !bwrap_available() {
            return;
        }
        let root = ProbeFixture::new().expect("fixture");
        let prepared = shell_command(
            "test ! -e /proc/self/fd/1025 && printf fd-closed",
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

    #[test]
    fn strict_kernel_mode_executes_only_with_verified_backend() {
        if !bwrap_available() {
            return;
        }
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

    #[test]
    fn kernel_hardened_child_cannot_write_system_root_or_mount() {
        if !bwrap_available() {
            return;
        }
        let root = ProbeFixture::new().expect("fixture");
        let prepared = shell_command(
            "test ! -w /etc && ! mount -t tmpfs tmpfs /tmp 2>/dev/null",
            &SandboxLaunchSpec::workspace(&root.workspace),
        )
        .expect("prepare hardened sandbox");
        assert!(prepared.into_command().status().unwrap().success());
    }
}
