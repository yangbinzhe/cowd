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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_and_get_session() {
        let mgr = UnifiedSessionManager::new();
        let meta = UnifiedSessionMeta {
            id: "s1".into(),
            session_type: SessionType::Cli,
            created_at: 1000,
            last_activity: 1000,
            workspace: None,
            platform_data: HashMap::new(),
        };
        mgr.register_session(meta).await;
        let got = mgr.get_session("s1").await.unwrap();
        assert_eq!(got.id, "s1");
        assert_eq!(got.session_type, SessionType::Cli);
    }

    #[tokio::test]
    async fn get_session_returns_none_for_missing() {
        let mgr = UnifiedSessionManager::new();
        assert!(mgr.get_session("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn list_sessions_returns_all() {
        let mgr = UnifiedSessionManager::new();
        mgr.register_session(test_meta("a", SessionType::Cli)).await;
        mgr.register_session(test_meta("b", SessionType::GatewayApi)).await;
        assert_eq!(mgr.list_sessions().await.len(), 2);
    }

    #[tokio::test]
    async fn list_by_type_filters_correctly() {
        let mgr = UnifiedSessionManager::new();
        mgr.register_session(test_meta("a", SessionType::Cli)).await;
        mgr.register_session(test_meta("b", SessionType::GatewayApi)).await;
        mgr.register_session(test_meta("c", SessionType::Cli)).await;

        let cli = mgr.list_sessions_by_type(SessionType::Cli).await;
        assert_eq!(cli.len(), 2);
        assert!(cli.iter().all(|m| m.session_type == SessionType::Cli));
    }

    #[tokio::test]
    async fn touch_session_updates_activity() {
        let mgr = UnifiedSessionManager::new();
        mgr.register_session(test_meta("s1", SessionType::Cli)).await;

        let before = mgr.get_session("s1").await.unwrap().last_activity;
        assert!(mgr.touch_session("s1").await);
        let after = mgr.get_session("s1").await.unwrap().last_activity;
        assert!(after >= before);
    }

    #[tokio::test]
    async fn touch_session_returns_false_for_missing() {
        let mgr = UnifiedSessionManager::new();
        assert!(!mgr.touch_session("missing").await);
    }

    #[tokio::test]
    async fn remove_session_deletes_and_returns_true() {
        let mgr = UnifiedSessionManager::new();
        mgr.register_session(test_meta("s1", SessionType::Cli)).await;
        assert!(mgr.remove_session("s1").await);
        assert!(mgr.get_session("s1").await.is_none());
    }

    #[tokio::test]
    async fn remove_session_returns_false_for_missing() {
        let mgr = UnifiedSessionManager::new();
        assert!(!mgr.remove_session("missing").await);
    }

    #[tokio::test]
    async fn default_session_set_and_get() {
        let mgr = UnifiedSessionManager::new();
        mgr.set_default_session("default-id".into()).await;
        assert_eq!(mgr.get_default_session().await, Some("default-id".into()));
    }

    #[tokio::test]
    async fn get_or_create_default_produces_id() {
        let mgr = UnifiedSessionManager::new();
        let id = mgr.get_or_create_default().await;
        assert!(id.starts_with("cli_"));
        assert!(mgr.get_session(&id).await.is_some());
    }

    #[tokio::test]
    async fn get_or_create_default_returns_same_id() {
        let mgr = UnifiedSessionManager::new();
        let id1 = mgr.get_or_create_default().await;
        let id2 = mgr.get_or_create_default().await;
        assert_eq!(id1, id2);
    }

    #[tokio::test]
    async fn cleanup_stale_removes_old_sessions() {
        let mgr = UnifiedSessionManager::new();
        mgr.register_session(test_meta("old", SessionType::Cli)).await;
        // Set last_activity far in the past
        {
            let mut sessions = mgr.sessions.write().await;
            if let Some(s) = sessions.get_mut("old") {
                s.last_activity = 0;
            }
        }
        let removed = mgr.cleanup_stale_sessions(3600).await;
        assert_eq!(removed, 1);
        assert!(mgr.get_session("old").await.is_none());
    }

    #[tokio::test]
    async fn cleanup_stale_keeps_recent() {
        let mgr = UnifiedSessionManager::new();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let mut meta = test_meta("recent", SessionType::Cli);
        meta.last_activity = now;
        mgr.register_session(meta).await;

        let removed = mgr.cleanup_stale_sessions(3600).await;
        assert_eq!(removed, 0);
        assert!(mgr.get_session("recent").await.is_some());
    }

    #[tokio::test]
    async fn session_counts_groups_by_type() {
        let mgr = UnifiedSessionManager::new();
        mgr.register_session(test_meta("a", SessionType::Cli)).await;
        mgr.register_session(test_meta("b", SessionType::Cli)).await;
        mgr.register_session(test_meta("c", SessionType::GatewayApi)).await;

        let counts = mgr.session_counts().await;
        assert_eq!(counts.get(&SessionType::Cli), Some(&2));
        assert_eq!(counts.get(&SessionType::GatewayApi), Some(&1));
    }

    #[tokio::test]
    async fn session_type_display() {
        assert_eq!(SessionType::Cli.to_string(), "cli");
        assert_eq!(SessionType::GatewayApi.to_string(), "gateway_api");
        assert_eq!(SessionType::GatewayFeishu.to_string(), "gateway_feishu");
    }

    #[test]
    fn create_session_manager_returns_arc() {
        let shared = create_session_manager();
        let _ = shared.get_or_create_default(); // drops Arc
    }

    fn test_meta(id: &str, st: SessionType) -> UnifiedSessionMeta {
        UnifiedSessionMeta {
            id: id.into(),
            session_type: st,
            created_at: 1000,
            last_activity: 1000,
            workspace: None,
            platform_data: HashMap::new(),
        }
    }
}
