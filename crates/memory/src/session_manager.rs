//! Unified Session Manager
//!
//! Provides unified session management for both CLI and Gateway.
//! This module bridges the gap between CLI's Runtime-based sessions
//! and Gateway's multi-platform session handling.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Session metadata for unified management
#[derive(Debug, Clone)]
pub struct UnifiedSessionMeta {
    /// Session identifier
    pub id: String,
    /// Session type (cli, gateway_api, gateway_feishu, etc.)
    pub session_type: SessionType,
    /// Creation timestamp (Unix seconds)
    pub created_at: u64,
    /// Last activity timestamp
    pub last_activity: u64,
    /// Associated workspace
    pub workspace: Option<PathBuf>,
    /// Platform-specific metadata
    pub platform_data: HashMap<String, String>,
}

/// Session type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionType {
    /// CLI session
    Cli,
    /// Gateway API session
    GatewayApi,
    /// Gateway Feishu session
    GatewayFeishu,
    /// Gateway WeChat session
    GatewayWechat,
    /// Gateway Email session
    GatewayEmail,
}

impl std::fmt::Display for SessionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SessionType::Cli => write!(f, "cli"),
            SessionType::GatewayApi => write!(f, "gateway_api"),
            SessionType::GatewayFeishu => write!(f, "gateway_feishu"),
            SessionType::GatewayWechat => write!(f, "gateway_wechat"),
            SessionType::GatewayEmail => write!(f, "gateway_email"),
        }
    }
}

/// Unified session manager for cross-platform session handling
pub struct UnifiedSessionManager {
    /// Active sessions
    sessions: RwLock<HashMap<String, UnifiedSessionMeta>>,
    /// Default session ID (for CLI mode)
    default_session: RwLock<Option<String>>,
}

impl UnifiedSessionManager {
    /// Create a new unified session manager
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            default_session: RwLock::new(None),
        }
    }

    /// Register a new session
    pub async fn register_session(&self, meta: UnifiedSessionMeta) {
        let mut sessions = self.sessions.write().await;
        sessions.insert(meta.id.clone(), meta);
    }

    /// Get session metadata
    pub async fn get_session(&self, id: &str) -> Option<UnifiedSessionMeta> {
        let sessions = self.sessions.read().await;
        sessions.get(id).cloned()
    }

    /// List all sessions
    pub async fn list_sessions(&self) -> Vec<UnifiedSessionMeta> {
        let sessions = self.sessions.read().await;
        sessions.values().cloned().collect()
    }

    /// List sessions by type
    pub async fn list_sessions_by_type(&self, session_type: SessionType) -> Vec<UnifiedSessionMeta> {
        let sessions = self.sessions.read().await;
        sessions
            .values()
            .filter(|m| m.session_type == session_type)
            .cloned()
            .collect()
    }

    /// Update session activity
    pub async fn touch_session(&self, id: &str) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.get_mut(id) {
            session.last_activity = now;
            true
        } else {
            false
        }
    }

    /// Remove a session
    pub async fn remove_session(&self, id: &str) -> bool {
        let mut sessions = self.sessions.write().await;
        sessions.remove(id).is_some()
    }

    /// Set default session (CLI mode)
    pub async fn set_default_session(&self, id: String) {
        let mut default = self.default_session.write().await;
        *default = Some(id);
    }

    /// Get default session
    pub async fn get_default_session(&self) -> Option<String> {
        let default = self.default_session.read().await;
        default.clone()
    }

    /// Get or create default session
    pub async fn get_or_create_default(&self) -> String {
        // Check if default exists
        if let Some(id) = self.get_default_session().await {
            return id;
        }

        // Create new default session
        let id = format!("cli_{}", uuid::Uuid::new_v4());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let meta = UnifiedSessionMeta {
            id: id.clone(),
            session_type: SessionType::Cli,
            created_at: now,
            last_activity: now,
            workspace: None,
            platform_data: HashMap::new(),
        };

        self.register_session(meta).await;
        self.set_default_session(id.clone()).await;
        id
    }

    /// Clean up stale sessions (older than max_age seconds)
    pub async fn cleanup_stale_sessions(&self, max_age_seconds: u64) -> usize {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let mut sessions = self.sessions.write().await;
        let before = sessions.len();

        sessions.retain(|_, meta| {
            now.saturating_sub(meta.last_activity) < max_age_seconds
        });

        before - sessions.len()
    }

    /// Get session count by type
    pub async fn session_counts(&self) -> HashMap<SessionType, usize> {
        let sessions = self.sessions.read().await;
        let mut counts = HashMap::new();

        for meta in sessions.values() {
            *counts.entry(meta.session_type).or_insert(0) += 1;
        }

        counts
    }
}

impl Default for UnifiedSessionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Wrapper for sharing UnifiedSessionManager across threads
pub type SharedSessionManager = Arc<UnifiedSessionManager>;

/// Create a new shared session manager
pub fn create_session_manager() -> SharedSessionManager {
    Arc::new(UnifiedSessionManager::new())
}

// ── Gateway Session Bridge ───────────────────────────────────────────────────

/// Bridge trait for Gateway to use unified session manager
#[async_trait::async_trait]
pub trait SessionBridge: Send + Sync {
    /// Get the unified session manager
    fn session_manager(&self) -> SharedSessionManager;

    /// Register a gateway session
    async fn register_gateway_session(
        &self,
        session_id: String,
        session_type: SessionType,
        platform_data: HashMap<String, String>,
    ) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let meta = UnifiedSessionMeta {
            id: session_id,
            session_type,
            created_at: now,
            last_activity: now,
            workspace: None,
            platform_data,
        };

        self.session_manager().register_session(meta).await;
    }

    /// Check if a session is active
    async fn is_session_active(&self, session_id: &str) -> bool {
        self.session_manager().get_session(session_id).await.is_some()
    }
}

// ── CLI Session Bridge ───────────────────────────────────────────────────────

/// Bridge trait for CLI to use unified session manager
pub trait CliSessionBridge {
    /// Get the unified session manager
    fn session_manager(&self) -> SharedSessionManager;

    /// Initialize CLI session
    fn init_cli_session(&self) -> String;
}

impl CliSessionBridge for SharedSessionManager {
    fn session_manager(&self) -> SharedSessionManager {
        self.clone()
    }

    fn init_cli_session(&self) -> String {
        // This needs to be async, so we use a runtime
        let rt = tokio::runtime::Handle::current();
        rt.block_on(async {
            self.get_or_create_default().await
        })
    }
}
