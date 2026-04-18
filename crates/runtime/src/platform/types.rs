//! Common types shared across platform adapters.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Unique identifier for a platform session.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionKey {
    /// The platform identifier (e.g., "feishu", "wecom", "email").
    pub platform: String,
    /// The user/session identifier on that platform.
    pub user_id: String,
    /// Optional conversation thread identifier.
    pub thread_id: Option<String>,
}

impl SessionKey {
    /// Create a new session key.
    pub fn new(platform: impl Into<String>, user_id: impl Into<String>) -> Self {
        Self {
            platform: platform.into(),
            user_id: user_id.into(),
            thread_id: None,
        }
    }

    /// Create a session key with a thread ID.
    pub fn with_thread(
        platform: impl Into<String>,
        user_id: impl Into<String>,
        thread_id: impl Into<String>,
    ) -> Self {
        Self {
            platform: platform.into(),
            user_id: user_id.into(),
            thread_id: Some(thread_id.into()),
        }
    }

    /// Convert to a string representation for logging/debugging.
    pub fn as_str(&self) -> String {
        match &self.thread_id {
            Some(thread) => format!("{}:{}:{}", self.platform, self.user_id, thread),
            None => format!("{}:{}", self.platform, self.user_id),
        }
    }
}

impl fmt::Display for SessionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for SessionKey {
    fn from(s: &str) -> Self {
        let parts: Vec<&str> = s.split(':').collect();
        match parts.len() {
            0 => Self::new("unknown", "unknown"),
            1 => Self::new(parts[0], "unknown"),
            2 => Self::new(parts[0], parts[1]),
            _ => Self::with_thread(parts[0], parts[1], parts[2]),
        }
    }
}

/// Session metadata associated with a platform session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlatformSession {
    /// The session key.
    pub key: SessionKey,
    /// Session creation timestamp.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Last activity timestamp.
    pub last_activity: chrono::DateTime<chrono::Utc>,
    /// Number of messages exchanged.
    pub message_count: usize,
    /// Optional user display name.
    pub display_name: Option<String>,
}

impl PlatformSession {
    /// Create a new platform session.
    pub fn new(key: SessionKey) -> Self {
        let now = chrono::Utc::now();
        Self {
            key,
            created_at: now,
            last_activity: now,
            message_count: 0,
            display_name: None,
        }
    }

    /// Update the last activity timestamp.
    pub fn touch(&mut self) {
        self.last_activity = chrono::Utc::now();
        self.message_count += 1;
    }

    /// Set the display name.
    pub fn with_display_name(mut self, name: impl Into<String>) -> Self {
        self.display_name = Some(name.into());
        self
    }
}
