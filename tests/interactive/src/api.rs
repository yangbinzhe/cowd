use anyhow::{Result, anyhow};
use std::process::Command;

/// Simple HTTP client via curl
pub struct ApiClient {
    pub base: String,
}

impl ApiClient {
    pub fn new(base: &str) -> Self {
        Self { base: base.to_string() }
    }

    pub fn get(&self, path: &str) -> Result<String> {
        let out = Command::new("curl")
            .args(["-s", &format!("{}{}", self.base, path)])
            .output()?;
        Ok(String::from_utf8_lossy(&out.stdout).to_string())
    }

    pub fn post(&self, path: &str) -> Result<String> {
        let out = Command::new("curl")
            .args(["-s", "-X", "POST", &format!("{}{}", self.base, path)])
            .output()?;
        Ok(String::from_utf8_lossy(&out.stdout).to_string())
    }

    pub fn get_json(&self, path: &str) -> Result<serde_json::Value> {
        let body = self.get(path)?;
        Ok(serde_json::from_str(&body)?)
    }
}
