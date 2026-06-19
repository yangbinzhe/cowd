use anyhow::{anyhow, Result};
use std::process::Command;
use std::time::{Duration, Instant};

pub struct TuiSession {
    session: String,
    closed: bool,
}

impl TuiSession {
    pub fn new(name: &str) -> Result<Self> {
        let session = owned_session_name(name);
        let status = Command::new("tmux")
            .args([
                "new-session",
                "-d",
                "-s",
                &session,
                &std::env::var("COWD_BIN").unwrap_or_else(|_| "cowd".to_string()),
            ])
            .status()?;
        if !status.success() {
            return Err(anyhow!("tmux session failed"));
        }
        Ok(Self {
            session,
            closed: false,
        })
    }

    pub fn send(&self, text: &str) -> Result<()> {
        Command::new("tmux").args(["send-keys", "-t", &self.session, "-l", text]).status()?;
        Ok(()) }
    pub fn enter(&self) -> Result<()> {
        Command::new("tmux").args(["send-keys", "-t", &self.session, "Enter"]).status()?;
        Ok(()) }
    pub fn send_key(&self, key: &str) -> Result<()> {
        Command::new("tmux").args(["send-keys", "-t", &self.session, key]).status()?;
        Ok(()) }
    pub fn send_ctrl(&self, ch: char) -> Result<()> {
        Command::new("tmux").args(["send-keys", "-t", &self.session, &format!("C-{ch}")]).status()?;
        Ok(()) }
    pub fn send_alt(&self, key: &str) -> Result<()> {
        Command::new("tmux").args(["send-keys", "-t", &self.session, &format!("M-{key}")]).status()?;
        Ok(()) }
    #[allow(dead_code)]
    pub fn send_shift_enter(&self) -> Result<()> {
        Command::new("tmux").args(["send-keys", "-t", &self.session, "S-Enter"]).status()?;
        Ok(()) }

    pub fn capture(&self) -> Result<String> {
        let out = Command::new("tmux").args(["capture-pane", "-t", &self.session, "-p", "-S", "-"]).output()?;
        Ok(String::from_utf8_lossy(&out.stdout).to_string())
    }

    pub fn wait_for(&self, expected: &str, secs: u64) -> Result<()> {
        let start = Instant::now();
        let t = Duration::from_secs(secs);
        while start.elapsed() < t {
            if self.capture()?.contains(expected) { return Ok(()); }
            std::thread::sleep(Duration::from_millis(200));
        }
        Err(anyhow!("Timeout waiting for '{expected}'"))
    }

    pub fn wait_until_ready(&self, secs: u64) -> Result<()> {
        let start = Instant::now();
        let t = Duration::from_secs(secs);
        while start.elapsed() < t {
            let cap = self.capture()?;
            if capture_is_healthy(&cap) && cap.trim().len() > 80 {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        Err(anyhow!("Timeout waiting for rendered TUI"))
    }

    pub fn assert_healthy_capture(&self, min_len: usize) -> Result<String> {
        let cap = self.capture()?;
        if !capture_is_healthy(&cap) {
            return Err(anyhow!("TUI capture contains startup/runtime failure"));
        }
        if cap.trim().len() < min_len {
            return Err(anyhow!("TUI capture too short ({} chars)", cap.trim().len()));
        }
        Ok(cap)
    }

    pub fn screenshot(&self, path: &str) -> Result<()> {
        let content = self.capture()?;
        std::fs::write(path, content)?;
        Ok(())
    }

    pub fn close(mut self) -> Result<()> {
        Command::new("tmux").args(["kill-session", "-t", &self.session]).status()?;
        self.closed = true;
        Ok(())
    }
}

fn owned_session_name(name: &str) -> String {
    let sanitized = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string();
    let base = if sanitized.is_empty() {
        "scenario".to_string()
    } else {
        sanitized
    };
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("cowd-it-{base}-{}-{nonce}", std::process::id())
}

fn capture_is_healthy(capture: &str) -> bool {
    let lower = capture.to_lowercase();
    !lower.contains("panic")
        && !lower.contains("backtrace")
        && !lower.contains("thread '")
        && !lower.contains("failed to initialize terminal")
        && !lower.contains("run cowd --help")
}

impl Drop for TuiSession {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        let _ = Command::new("tmux")
            .args(["kill-session", "-t", &self.session])
            .status();
    }
}
