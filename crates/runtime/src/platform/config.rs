//! Platform configuration.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for a single platform instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlatformConfig {
    /// Whether this platform is enabled.
    pub enabled: bool,
    /// Platform type (feishu, wecom, email).
    pub platform_type: String,
    /// Platform-specific settings.
    #[serde(default)]
    pub settings: HashMap<String, serde_json::Value>,
}

impl PlatformConfig {
    /// Create a new platform config.
    pub fn new(platform_type: impl Into<String>) -> Self {
        Self {
            enabled: true,
            platform_type: platform_type.into(),
            settings: HashMap::new(),
        }
    }

    /// Set a setting value.
    pub fn with_setting(mut self, key: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        self.settings.insert(key.into(), value.into());
        self
    }

    /// Get a setting value.
    pub fn get_setting(&self, key: &str) -> Option<&serde_json::Value> {
        self.settings.get(key)
    }

    /// Get a setting as a specific type.
    pub fn get_setting_as<T: serde::de::DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.settings.get(key).and_then(|v| serde_json::from_value(v.clone()).ok())
    }
}

/// Session reset policy for platform sessions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionResetPolicy {
    /// Reset sessions daily.
    Daily,
    /// Reset sessions after idle timeout.
    Idle,
    /// Reset sessions on either condition.
    Both,
    /// Never reset sessions automatically.
    None,
}

impl Default for SessionResetPolicy {
    fn default() -> Self {
        Self::Daily
    }
}

/// Runtime-wide platform configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlatformRuntimeConfig {
    /// Incoming message channel capacity.
    pub channel_capacity: usize,
    /// Session reset policy.
    pub session_reset: SessionResetPolicy,
    /// Retry configuration.
    pub retry: RetryConfig,
    /// Individual platform configurations.
    pub platforms: Vec<PlatformConfig>,
}

impl Default for PlatformRuntimeConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 256,
            session_reset: SessionResetPolicy::default(),
            retry: RetryConfig::default(),
            platforms: Vec::new(),
        }
    }
}

/// Retry configuration for failed operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retries.
    pub max_retries: u32,
    /// Initial backoff delay in milliseconds.
    pub initial_delay_ms: u64,
    /// Maximum backoff delay in milliseconds.
    pub max_delay_ms: u64,
    /// Backoff multiplier.
    pub multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay_ms: 100,
            max_delay_ms: 5000,
            multiplier: 2.0,
        }
    }
}
