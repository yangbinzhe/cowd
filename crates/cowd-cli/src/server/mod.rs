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
    let runtime_dir = std::env::var("XDG_RUNTIME_DIR")
        .unwrap_or_else(|_| format!("/tmp/cowd-{}", std::env::var("USER").unwrap_or_else(|_| "unknown".to_string())));
    let dir = PathBuf::from(runtime_dir);
    let _ = std::fs::create_dir_all(&dir);
    dir.join("cowd-serve.pid")
}

pub fn addr_file() -> PathBuf {
    pid_file().with_extension("addr")
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerInfo {
    pub pid: u32,
    pub address: String,
}

pub fn get_server_status() -> Result<Option<ServerInfo>, ServerError> {
    let pid_path = pid_file();
    if !pid_path.exists() {
        return Ok(None);
    }

    let pid: u32 = fs::read_to_string(&pid_path)?
        .trim()
        .parse()?;

    if pid == 0 || !process_exists(pid) {
        fs::remove_file(&pid_path).ok();
        return Ok(None);
    }

    let address = std::fs::read_to_string(addr_file())
        .unwrap_or_else(|_| "http://127.0.0.1:8642".to_string());

    Ok(Some(ServerInfo {
        pid,
        address,
    }))
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
            std::process::Command::new("kill")
                .arg("-TERM")
                .arg(info.pid.to_string())
                .output()?;
        }
        fs::remove_file(pid_file())?;
        let _ = std::fs::remove_file(addr_file());
    }
    Ok(())
}


