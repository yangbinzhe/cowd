#![allow(dead_code)]
//! Gateway server — service management (pid, status, start/stop).

use std::{fmt, fs, path::PathBuf};

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
    let _ = std::fs::create_dir_all(&dir);
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
    let _ = std::fs::create_dir_all(&dir);
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

        let address = std::fs::read_to_string(pid_path.with_extension("addr"))
            .unwrap_or_else(|_| "http://127.0.0.1:8642".to_string());

        return Ok(Some(ServerInfo { pid, address }));
    }

    Ok(discover_default_gateway_listener())
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
    if let Some(info) = get_server_status()? {
        #[cfg(unix)]
        {
            if info.pid != 0 {
                std::process::Command::new("kill")
                    .arg("-TERM")
                    .arg(info.pid.to_string())
                    .output()?;
            }
        }
    }
    for pid_path in status_pid_files() {
        let _ = fs::remove_file(&pid_path);
        let _ = std::fs::remove_file(pid_path.with_extension("addr"));
    }
    Ok(())
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
    if pid != 0 {
        let _ = fs::write(pid_file(), pid.to_string());
        let _ = fs::write(addr_file(), "http://127.0.0.1:8642");
    }
    Some(ServerInfo {
        pid,
        address: "http://127.0.0.1:8642".to_string(),
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
}
