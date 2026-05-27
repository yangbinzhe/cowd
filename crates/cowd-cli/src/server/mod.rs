#![allow(dead_code)]
//! Gateway server — service management (pid, status, start/stop).

use std::{fmt, fs, path::PathBuf};

use serde::Serialize;
use runtime::platform::PlatformConfig;
use runtime::{ApprovalConfig, SessionResetPolicy};

use memory::MemoryConfig;

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

    Ok(Some(ServerInfo {
        pid,
        address: "http://127.0.0.1:8642".to_string(),
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
    }
    Ok(())
}

// ── HTTP config ──────────────────────────────────────────────────

#[derive(Clone)]
pub struct HttpConfig {
    pub host: String,
    pub port: u16,
    pub auth_enabled: bool,
    pub auth_token: String,
    pub with_webui: bool,
    pub memory_config: Option<MemoryConfig>,
    pub session_store_path: Option<PathBuf>,
    pub platform_configs: Vec<PlatformConfig>,
    pub cors_origins: Vec<String>,
    pub approval_config: Option<ApprovalConfig>,
    pub session_reset: SessionResetPolicy,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8642,
            auth_enabled: true,
            auth_token: String::new(),
            with_webui: true,
            memory_config: None,
            session_store_path: None,
            platform_configs: Vec::new(),
            cors_origins: Vec::new(),
            approval_config: None,
            session_reset: SessionResetPolicy::None,
        }
    }
}
