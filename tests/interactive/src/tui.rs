use anyhow::{Result, anyhow};
use std::process::Command;
use std::time::{Duration, Instant};

pub struct TuiSession { session: String }

impl TuiSession {
    pub fn new(name: &str) -> Result<Self> {
        let _ = Command::new("tmux").args(["kill-session", "-t", name]).output();
        let status = Command::new("tmux")
            .args(["new-session", "-d", "-s", name, &format!("{}", std::env::var("COWD_BIN").unwrap_or_else(|_| "cowd".to_string()))]).status()?;
        if !status.success() { return Err(anyhow!("tmux session failed")); }
        Ok(Self { session: name.to_string() })
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

    pub fn close(self) -> Result<()> {
        Command::new("tmux").args(["kill-session", "-t", &self.session]).status()?;
        Ok(())
    }
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
        let _ = Command::new("tmux")
            .args(["kill-session", "-t", &self.session])
            .status();
    }
}
