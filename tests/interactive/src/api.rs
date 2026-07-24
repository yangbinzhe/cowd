use anyhow::{anyhow, Result};
use std::process::Command;
use std::time::{Duration, Instant};

/// Simple HTTP client via curl
pub struct ApiClient {
    pub base: String,
}

impl ApiClient {
    pub fn new(base: &str) -> Self {
        Self {
            base: base.to_string(),
        }
    }

    pub fn get(&self, path: &str) -> Result<String> {
        let out = Command::new("curl")
            .args(["-fsS", &format!("{}{}", self.base, path)])
            .output()?;
        if !out.status.success() {
            return Err(anyhow!(
                "GET {}{} failed with status {}: {}",
                self.base,
                path,
                out.status,
                String::from_utf8_lossy(&out.stderr)
            ));
        }
        Ok(String::from_utf8_lossy(&out.stdout).to_string())
    }

    pub fn post(&self, path: &str) -> Result<String> {
        let out = Command::new("curl")
            .args(["-fsS", "-X", "POST", &format!("{}{}", self.base, path)])
            .output()?;
        if !out.status.success() {
            return Err(anyhow!(
                "POST {}{} failed with status {}: {}",
                self.base,
                path,
                out.status,
                String::from_utf8_lossy(&out.stderr)
            ));
        }
        Ok(String::from_utf8_lossy(&out.stdout).to_string())
    }

    #[allow(dead_code)]
    pub fn get_json(&self, path: &str) -> Result<serde_json::Value> {
        let body = self.get(path)?;
        Ok(serde_json::from_str(&body)?)
    }

    /// Poll a GET endpoint until the response satisfies `predicate` or `timeout` expires.
    /// Polls every 200ms. Returns Ok when the predicate returns true.
    #[allow(dead_code)]
    pub fn poll_until<F>(&self, path: &str, predicate: F, timeout: Duration) -> Result<()>
    where
        F: Fn(&str) -> bool,
    {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if let Ok(body) = self.get(path) {
                if predicate(&body) {
                    return Ok(());
                }
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        Err(anyhow!("poll_until timeout ({timeout:?}) for GET {path}"))
    }
}
