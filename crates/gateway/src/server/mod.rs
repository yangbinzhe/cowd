#![allow(dead_code)]
//! Gateway server — service management (pid, status, start/stop).

use std::{
    fmt, fs,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::time::{Duration, Instant};

use serde::Serialize;

// ── Error type ───────────────────────────────────────────────────

#[derive(Debug)]
pub struct ServerError(String);

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ServerError {}

impl From<std::io::Error> for ServerError {
    fn from(e: std::io::Error) -> Self {
        ServerError(e.to_string())
    }
}

impl From<std::num::ParseIntError> for ServerError {
    fn from(e: std::num::ParseIntError) -> Self {
        ServerError(e.to_string())
    }
}

// ── Service management ───────────────────────────────────────────

pub fn pid_file() -> PathBuf {
    let dir = runtime::cowd_dirs::config_home_dir().join("run");
    if let Err(error) = std::fs::create_dir_all(&dir) {
        tracing::warn!(path = %dir.display(), %error, "gateway run directory is unavailable");
    }
    dir.join("cowd-serve.pid")
}

fn legacy_runtime_pid_file() -> PathBuf {
    let runtime_dir = std::env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| {
        format!(
            "/tmp/cowd-{}",
            std::env::var("USER").unwrap_or_else(|_| "unknown".to_string())
        )
    });
    let dir = PathBuf::from(runtime_dir);
    if let Err(error) = std::fs::create_dir_all(&dir) {
        tracing::warn!(path = %dir.display(), %error, "legacy gateway run directory is unavailable");
    }
    dir.join("cowd-serve.pid")
}

pub fn addr_file() -> PathBuf {
    pid_file().with_extension("addr")
}

fn status_pid_files() -> Vec<PathBuf> {
    let primary = pid_file();
    let legacy = legacy_runtime_pid_file();
    if legacy == primary {
        vec![primary]
    } else {
        vec![primary, legacy]
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerInfo {
    pub pid: u32,
    pub address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discovery_warning: Option<String>,
}

pub fn get_server_status() -> Result<Option<ServerInfo>, ServerError> {
    for pid_path in status_pid_files() {
        if !pid_path.exists() {
            continue;
        }

        let pid: u32 = match fs::read_to_string(&pid_path)?.trim().parse() {
            Ok(pid) => pid,
            Err(_) => {
                fs::remove_file(&pid_path).ok();
                let _ = std::fs::remove_file(pid_path.with_extension("addr"));
                continue;
            }
        };

        if pid == 0 || !process_exists(pid) {
            fs::remove_file(&pid_path).ok();
            let _ = std::fs::remove_file(pid_path.with_extension("addr"));
            continue;
        }

        let (address, discovery_warning) =
            match std::fs::read_to_string(pid_path.with_extension("addr")) {
                Ok(address) => (address.trim().to_string(), None),
                Err(error) => (
                    "http://127.0.0.1:8642".to_string(),
                    Some(format!(
                        "gateway process is running but address metadata is unavailable: {error}"
                    )),
                ),
            };

        return Ok(Some(ServerInfo {
            pid,
            address,
            discovery_warning,
        }));
    }

    if config_home_overridden() {
        return Ok(None);
    }

    Ok(discover_default_gateway_listener())
}

fn config_home_overridden() -> bool {
    std::env::var_os("COWD_CONFIG_HOME").is_some()
}

#[cfg(unix)]
fn process_exists(pid: u32) -> bool {
    std::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn process_exists(_pid: u32) -> bool {
    true
}

pub fn stop_server() -> Result<(), ServerError> {
    #[cfg(unix)]
    {
        // Never terminate a listener discovered from another Cowd binary or
        // worktree. Service ownership is the current executable plus the
        // exact `gateway run` command line, not merely port 8642. Managed
        // Gateway processes are process-group leaders, so signal their group
        // to stop owned helpers such as the auth broker at the same boundary.
        let processes = discover_current_exe_gateway_run_processes()
            .into_iter()
            .filter(|pid| *pid != std::process::id())
            .map(ManagedGatewayProcess::inspect)
            .collect::<std::collections::BTreeSet<_>>();
        for process in &processes {
            process.signal(libc::SIGTERM)?;
        }
        let remaining = wait_for_processes_to_exit(&processes, Duration::from_secs(3));
        for process in &remaining {
            process.signal(libc::SIGKILL)?;
        }
        let remaining = wait_for_processes_to_exit(&remaining, Duration::from_secs(1));
        if !remaining.is_empty() {
            return Err(ServerError(format!(
                "gateway process did not exit: {}",
                remaining
                    .iter()
                    .map(|process| process.pid.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
    }
    for pid_path in status_pid_files() {
        remove_status_file_if_present(&pid_path)?;
        remove_status_file_if_present(&pid_path.with_extension("addr"))?;
    }
    Ok(())
}

fn remove_status_file_if_present(path: &Path) -> Result<(), ServerError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ServerError(format!(
            "failed to remove gateway status file `{}`: {error}",
            path.display()
        ))),
    }
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ManagedGatewayProcess {
    pid: u32,
    process_group: Option<i32>,
}

#[cfg(unix)]
impl ManagedGatewayProcess {
    fn inspect(pid: u32) -> Self {
        let Ok(pid_i32) = i32::try_from(pid) else {
            return Self {
                pid,
                process_group: None,
            };
        };
        let process_group = unsafe { libc::getpgid(pid_i32) };
        Self {
            pid,
            process_group: (process_group == pid_i32).then_some(process_group),
        }
    }

    fn signal(self, signal: i32) -> Result<(), ServerError> {
        let target = match self.process_group {
            Some(process_group) => -process_group,
            None => i32::try_from(self.pid).map_err(|_| {
                ServerError(format!("gateway PID {} exceeds platform range", self.pid))
            })?,
        };
        if unsafe { libc::kill(target, signal) } == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(())
        } else {
            Err(ServerError(format!(
                "failed to signal gateway process tree {}: {error}",
                self.pid
            )))
        }
    }

    fn exists(self) -> bool {
        let target = match self.process_group {
            Some(process_group) => -process_group,
            None => {
                let Ok(pid) = i32::try_from(self.pid) else {
                    return false;
                };
                pid
            }
        };
        if unsafe { libc::kill(target, 0) } == 0 {
            return true;
        }
        std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }
}

#[cfg(unix)]
fn wait_for_processes_to_exit(
    processes: &std::collections::BTreeSet<ManagedGatewayProcess>,
    timeout: Duration,
) -> std::collections::BTreeSet<ManagedGatewayProcess> {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = processes
            .iter()
            .copied()
            .filter(|process| process.exists())
            .collect::<std::collections::BTreeSet<_>>();
        if remaining.is_empty() || Instant::now() >= deadline {
            return remaining;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(unix)]
fn discover_current_exe_gateway_run_processes() -> Vec<u32> {
    let current_exe = match std::env::current_exe()
        .ok()
        .and_then(|path| fs::canonicalize(path).ok())
    {
        Some(path) => path,
        None => return Vec::new(),
    };

    let Ok(entries) = fs::read_dir("/proc") else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let pid = entry.file_name().to_string_lossy().parse::<u32>().ok()?;
            if pid == std::process::id() {
                return None;
            }
            let proc_dir = entry.path();
            let cmdline = fs::read(proc_dir.join("cmdline")).ok()?;
            if !cmdline_is_gateway_run(&cmdline) {
                return None;
            }
            let executable_matches = fs::read_link(proc_dir.join("exe"))
                .ok()
                .and_then(|path| fs::canonicalize(path).ok())
                .is_some_and(|path| path == current_exe);
            let launch_path_matches = cmdline_executable_matches(&cmdline, &current_exe);
            (executable_matches || launch_path_matches).then_some(pid)
        })
        .collect()
}

#[cfg(unix)]
fn cmdline_is_gateway_run(cmdline: &[u8]) -> bool {
    let args: Vec<&[u8]> = cmdline
        .split(|byte| *byte == 0)
        .filter(|arg| !arg.is_empty())
        .collect();
    args.windows(2)
        .any(|pair| pair[0] == b"gateway" && pair[1] == b"run")
}

/// A running executable may be replaced during deployment, turning
/// `/proc/<pid>/exe` into a deleted inode. The command-line launch path stays
/// stable, so use it as a second ownership proof in addition to the inode.
#[cfg(unix)]
fn cmdline_executable_matches(cmdline: &[u8], current_exe: &Path) -> bool {
    let Some(raw_path) = cmdline.split(|byte| *byte == 0).find(|arg| !arg.is_empty()) else {
        return false;
    };
    let Ok(raw_path) = std::str::from_utf8(raw_path) else {
        return false;
    };
    let launch_path = Path::new(raw_path);
    launch_path == current_exe
        || fs::canonicalize(launch_path)
            .ok()
            .is_some_and(|path| path == current_exe)
}

#[cfg(unix)]
fn discover_default_gateway_listener() -> Option<ServerInfo> {
    let output = std::process::Command::new("ss")
        .args(["-ltnp"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout
        .lines()
        .find(|line| line.contains("127.0.0.1:8642") && line.contains("cowd"))?;
    let pid = extract_pid_from_ss_line(line).unwrap_or(0);
    let mut discovery_warnings = Vec::new();
    if pid != 0 {
        if let Err(error) = fs::write(pid_file(), pid.to_string()) {
            discovery_warnings.push(format!("cannot persist discovered gateway PID: {error}"));
        }
        if let Err(error) = fs::write(addr_file(), "http://127.0.0.1:8642") {
            discovery_warnings.push(format!(
                "cannot persist discovered gateway address: {error}"
            ));
        }
    }
    Some(ServerInfo {
        pid,
        address: "http://127.0.0.1:8642".to_string(),
        discovery_warning: (!discovery_warnings.is_empty()).then(|| discovery_warnings.join("; ")),
    })
}

#[cfg(not(unix))]
fn discover_default_gateway_listener() -> Option<ServerInfo> {
    None
}

#[cfg(unix)]
fn extract_pid_from_ss_line(line: &str) -> Option<u32> {
    let marker = "pid=";
    let start = line.find(marker)? + marker.len();
    let digits: String = line[start..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect();
    digits.parse().ok()
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    #[test]
    fn parses_pid_from_ss_listener_line() {
        let line = r#"LISTEN 0 128 127.0.0.1:8642 0.0.0.0:* users:(("cowd",pid=1834062,fd=35))"#;
        assert_eq!(super::extract_pid_from_ss_line(line), Some(1834062));
    }

    #[test]
    fn missing_pid_in_ss_line_returns_none() {
        assert_eq!(
            super::extract_pid_from_ss_line("LISTEN 127.0.0.1:8642"),
            None
        );
    }

    #[cfg(unix)]
    #[test]
    fn detects_gateway_run_cmdline() {
        assert!(super::cmdline_is_gateway_run(b"/tmp/cowd\0gateway\0run\0"));
        assert!(!super::cmdline_is_gateway_run(
            b"/tmp/cowd\0gateway\0stop\0"
        ));
    }

    #[cfg(unix)]
    #[test]
    fn matches_gateway_launch_path_after_binary_replacement() {
        let current = std::path::Path::new("/opt/cowd/cowd");
        assert!(super::cmdline_executable_matches(
            b"/opt/cowd/cowd\0gateway\0run\0",
            current,
        ));
        assert!(!super::cmdline_executable_matches(
            b"/other-worktree/cowd\0gateway\0run\0",
            current,
        ));
    }

    #[cfg(unix)]
    #[test]
    fn managed_gateway_signal_stops_the_owned_process_group() {
        use std::os::unix::process::CommandExt;

        let mut child = std::process::Command::new("sh")
            .args(["-c", "sleep 30 & wait"])
            .process_group(0)
            .spawn()
            .expect("spawn isolated process group");
        let process = super::ManagedGatewayProcess::inspect(child.id());
        assert_eq!(
            process.process_group,
            i32::try_from(child.id()).ok(),
            "managed Gateway must be its process-group leader"
        );

        process
            .signal(libc::SIGTERM)
            .expect("signal managed process group");
        let remaining = super::wait_for_processes_to_exit(
            &std::collections::BTreeSet::from([process]),
            std::time::Duration::from_secs(2),
        );
        if !remaining.is_empty() {
            process
                .signal(libc::SIGKILL)
                .expect("force-stop managed process group");
        }
        let _ = child.wait();
        assert!(
            super::wait_for_processes_to_exit(
                &std::collections::BTreeSet::from([process]),
                std::time::Duration::from_secs(1),
            )
            .is_empty(),
            "Gateway stop must not leave owned helper processes alive"
        );
    }

    #[test]
    #[serial]
    fn config_home_override_disables_default_gateway_discovery() {
        let _env_guard = crate::test_process_env_lock();
        let original_config_home = std::env::var_os("COWD_CONFIG_HOME");
        let original_xdg_runtime = std::env::var_os("XDG_RUNTIME_DIR");
        let config_home = tempfile::tempdir().unwrap();
        let xdg_runtime = tempfile::tempdir().unwrap();
        std::env::set_var("COWD_CONFIG_HOME", config_home.path());
        std::env::set_var("XDG_RUNTIME_DIR", xdg_runtime.path());

        assert!(super::get_server_status().unwrap().is_none());

        match original_config_home {
            Some(value) => std::env::set_var("COWD_CONFIG_HOME", value),
            None => std::env::remove_var("COWD_CONFIG_HOME"),
        }
        match original_xdg_runtime {
            Some(value) => std::env::set_var("XDG_RUNTIME_DIR", value),
            None => std::env::remove_var("XDG_RUNTIME_DIR"),
        }
    }
}
