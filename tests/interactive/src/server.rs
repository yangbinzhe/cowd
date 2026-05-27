use anyhow::{Result, anyhow};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

pub struct ServerProcess {
    proc: Option<Child>,
}

impl ServerProcess {
    pub fn start() -> Result<Self> {
        let cowd = std::env::var("COWD_BIN").unwrap_or_else(|_| "cowd".to_string());
        let child = Command::new(&cowd)
            .args(["serve", "--port", "8642", "--no-auth"])
            .spawn()
            .map_err(|e| anyhow!("Failed to start cowd serve: {e}"))?;
        let mut srv = Self { proc: Some(child) };
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(15) {
            let out = Command::new("curl")
                .args(["-s", "http://127.0.0.1:8642/health"])
                .output();
            if let Ok(o) = out {
                let body = String::from_utf8_lossy(&o.stdout);
                if o.status.success() {
                    return Ok(srv);
                }
            }
            std::thread::sleep(Duration::from_millis(300));
        }
        srv.close()?;
        Err(anyhow!("Server did not become ready within 15s"))
    }

    pub fn close(&mut self) -> Result<()> {
        if let Some(mut child) = self.proc.take() {
            child.kill()?;
            child.wait()?;
        }
        Ok(())
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.proc.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        // Also kill gateway if running
        let _ = Command::new("pkill")
            .args(["-f", "cowd.*gateway"])
            .status();
    }
}
