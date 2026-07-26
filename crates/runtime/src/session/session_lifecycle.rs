use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

// ── Eviction Policy ───────────────────────────────────────────────────────

/// Policy used when the active session count exceeds the configured maximum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum EvictionPolicy {
    /// Evict the least-recently-used session.
    #[default]
    Lru,
    /// Evict the oldest session by creation time.
    Oldest,
    /// Evict the session with the largest message count.
    Largest,
}

impl Display for EvictionPolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lru => write!(f, "lru"),
            Self::Oldest => write!(f, "oldest"),
            Self::Largest => write!(f, "largest"),
        }
    }
}

// ── Session Status ─────────────────────────────────────────────────────────

/// Lifecycle status assigned to a managed session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionStatus {
    /// Session is receiving or has recently received activity.
    Active,
    /// Session has not been active within the configured idle timeout.
    Idle,
    /// Session has exceeded its maximum TTL.
    Expired,
    /// Session was forcibly removed due to capacity pressure.
    Evicted,
    /// Session was explicitly closed by the caller.
    Closed,
}

impl Display for SessionStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Idle => write!(f, "idle"),
            Self::Expired => write!(f, "expired"),
            Self::Evicted => write!(f, "evicted"),
            Self::Closed => write!(f, "closed"),
        }
    }
}

// ── Configuration ──────────────────────────────────────────────────────────

/// Top-level configuration for the [`SessionWorkingSetManager`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionLifecycleConfig {
    /// If set, a session that has not been marked active for this duration
    /// transitions to [`SessionStatus::Idle`].
    pub idle_timeout: Option<Duration>,
    /// If set, a session older than this duration transitions to
    /// [`SessionStatus::Expired`].
    pub max_ttl: Option<Duration>,
    /// Maximum number of concurrently active sessions before the manager
    /// triggers eviction.
    pub max_active_sessions: usize,
    /// Policy used to select the victim during eviction.
    pub eviction_policy: EvictionPolicy,
    /// Interval at which the internal background cleanup task runs.
    pub cleanup_interval: Duration,
}

impl Default for SessionLifecycleConfig {
    fn default() -> Self {
        Self {
            idle_timeout: Some(Duration::from_secs(300)),
            max_ttl: Some(Duration::from_secs(86_400)),
            max_active_sessions: 1024,
            eviction_policy: EvictionPolicy::default(),
            cleanup_interval: Duration::from_secs(30),
        }
    }
}

// ── Session Entry ──────────────────────────────────────────────────────────

/// Per-session metadata tracked by the lifecycle manager.
#[derive(Debug, Clone)]
struct SessionEntry {
    /// Wall-clock timestamp (millis) when the session was registered.
    created_at_ms: u64,
    /// Monotonic instant of the last `mark_active` call.
    last_active: Instant,
    /// Total number of conversation messages stored.
    message_count: usize,
    /// Current lifecycle status.
    status: SessionStatus,
}

impl SessionEntry {
    fn new() -> Self {
        Self {
            created_at_ms: current_time_millis(),
            last_active: Instant::now(),
            message_count: 0,
            status: SessionStatus::Active,
        }
    }
}

// ── Manager ────────────────────────────────────────────────────────────────

/// In-memory session lifecycle manager responsible for TTL enforcement,
/// idle detection, capacity-based eviction, and graceful close.
///
/// All mutable state is protected by an internal [`tokio::sync::RwLock`].
#[derive(Debug, Clone)]
pub struct SessionWorkingSetManager {
    sessions: Arc<RwLock<HashMap<String, SessionEntry>>>,
    config: SessionLifecycleConfig,
}

impl SessionWorkingSetManager {
    /// Create a new manager with the given configuration.
    #[must_use]
    pub fn new(config: SessionLifecycleConfig) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Register a session and assign it an initial [`SessionStatus::Active`].
    ///
    /// Returns `false` when the session is already tracked.
    pub async fn register(&self, session_id: &str) -> bool {
        let mut sessions = self.sessions.write().await;
        if sessions.contains_key(session_id) {
            return false;
        }
        sessions.insert(session_id.to_string(), SessionEntry::new());
        true
    }

    /// Signal that a session has received activity, resetting the idle timer.
    ///
    /// Does nothing when the session is not tracked or has already been
    /// evicted / closed.
    pub async fn mark_active(&self, session_id: &str) {
        let mut sessions = self.sessions.write().await;
        if let Some(entry) = sessions.get_mut(session_id) {
            entry.last_active = Instant::now();
            if entry.status == SessionStatus::Idle {
                entry.status = SessionStatus::Active;
            }
        }
    }

    /// Set the authoritative message count for a tracked session.
    ///
    /// Does nothing when the session is not tracked or has already been
    /// evicted / closed.
    pub async fn update_message_count(&self, session_id: &str, count: usize) {
        let mut sessions = self.sessions.write().await;
        if let Some(entry) = sessions.get_mut(session_id) {
            entry.message_count = count;
        }
    }

    /// Evaluate a session's lifecycle status without mutating it.
    ///
    /// Returns `None` when the session is unknown.
    #[must_use]
    pub async fn check_session(&self, session_id: &str) -> Option<SessionStatus> {
        let sessions = self.sessions.read().await;
        sessions.get(session_id).map(|entry| entry.status)
    }

    /// Remove a tracked session from the manager.
    ///
    /// Returns `true` when the session was present and removed; `false`
    /// when it was already absent.
    pub async fn unregister(&self, session_id: &str) -> bool {
        let mut sessions = self.sessions.write().await;
        sessions.remove(session_id).is_some()
    }

    /// Explicitly close a session, transitioning it to [`SessionStatus::Closed`]
    /// and removing it from tracked state.
    ///
    /// Returns `true` when the session was present and closed; `false`
    /// when it was already absent.
    pub async fn close(&self, session_id: &str) -> bool {
        let mut sessions = self.sessions.write().await;
        if let Some(entry) = sessions.get_mut(session_id) {
            entry.status = SessionStatus::Closed;
        }
        sessions.remove(session_id).is_some()
    }

    /// Return the number of currently tracked sessions.
    #[must_use]
    pub async fn active_count(&self) -> usize {
        self.sessions.read().await.len()
    }

    /// Return a snapshot of all session IDs together with their current status.
    #[must_use]
    pub async fn status_snapshot(&self) -> Vec<(String, SessionStatus)> {
        let sessions = self.sessions.read().await;
        sessions
            .iter()
            .map(|(id, entry)| (id.clone(), entry.status))
            .collect()
    }

    /// Select and remove one hot-runtime victim using the configured capacity
    /// policy. Durable Session identity is outside this resource manager and
    /// remains available for a later cold recovery.
    pub async fn evict_one_for_capacity(&self) -> Option<String> {
        let mut sessions = self.sessions.write().await;
        let victim = match self.config.eviction_policy {
            EvictionPolicy::Lru => sessions
                .iter()
                .min_by_key(|(_, entry)| entry.last_active)
                .map(|(id, _)| id.clone()),
            EvictionPolicy::Oldest => sessions
                .iter()
                .min_by_key(|(_, entry)| entry.created_at_ms)
                .map(|(id, _)| id.clone()),
            EvictionPolicy::Largest => sessions
                .iter()
                .max_by_key(|(_, entry)| entry.message_count)
                .map(|(id, _)| id.clone()),
        }?;
        sessions.remove(&victim);
        Some(victim)
    }

    /// Background cleanup: expire stale sessions and evict when over capacity.
    ///
    /// Call this periodically (e.g. from a tokio interval task) with the
    /// configured `cleanup_interval`.
    pub async fn run_cleanup(&self) -> Vec<String> {
        let now_instant = Instant::now();
        let now_ms = current_time_millis();

        let mut sessions = self.sessions.write().await;

        // Phase 1: expire sessions exceeding max TTL or idle timeout.
        for entry in sessions.values_mut() {
            match entry.status {
                SessionStatus::Active | SessionStatus::Idle => {
                    // Check TTL expiration.
                    if let Some(max_ttl) = self.config.max_ttl {
                        let age_ms = now_ms.saturating_sub(entry.created_at_ms);
                        if age_ms >= max_ttl.as_millis() as u64 {
                            entry.status = SessionStatus::Expired;
                            continue;
                        }
                    }
                    // Check idle timeout.
                    if let Some(idle_timeout) = self.config.idle_timeout {
                        if now_instant.duration_since(entry.last_active) >= idle_timeout {
                            entry.status = SessionStatus::Idle;
                        }
                    }
                }
                SessionStatus::Expired | SessionStatus::Evicted | SessionStatus::Closed => {}
            }
        }

        // Phase 2: evict sessions when over capacity.
        let excess = sessions
            .len()
            .saturating_sub(self.config.max_active_sessions);
        if excess == 0 {
            return Vec::new();
        }

        // Remove already-expired / closed entries first as they are
        // effectively dead weight.
        let dead_ids: Vec<String> = sessions
            .iter()
            .filter(|(_, entry)| {
                matches!(entry.status, SessionStatus::Expired | SessionStatus::Closed)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in &dead_ids {
            sessions.remove(id);
        }
        let remaining_excess = sessions
            .len()
            .saturating_sub(self.config.max_active_sessions);
        if remaining_excess == 0 {
            return dead_ids;
        }

        // Apply the configured eviction policy to the remaining sessions.
        let candidate_ids: Vec<String> = {
            let mut candidates: Vec<(String, &SessionEntry)> = sessions
                .iter()
                .map(|(id, entry)| (id.clone(), entry))
                .collect();

            match self.config.eviction_policy {
                EvictionPolicy::Lru => {
                    candidates.sort_by_key(|(_, entry)| entry.last_active);
                }
                EvictionPolicy::Oldest => {
                    candidates.sort_by_key(|(_, entry)| entry.created_at_ms);
                }
                EvictionPolicy::Largest => {
                    candidates.sort_by_key(|(_, entry)| entry.message_count);
                    candidates.reverse();
                }
            }

            candidates
                .into_iter()
                .take(remaining_excess)
                .map(|(id, _)| id)
                .collect()
        };

        for id in &candidate_ids {
            if let Some(entry) = sessions.get_mut(id) {
                entry.status = SessionStatus::Evicted;
            }
            sessions.remove(id);
        }
        dead_ids.into_iter().chain(candidate_ids).collect()
    }
}

impl Default for SessionWorkingSetManager {
    fn default() -> Self {
        Self::new(SessionLifecycleConfig::default())
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or_default()
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn config_for_tests() -> SessionLifecycleConfig {
        SessionLifecycleConfig {
            idle_timeout: Some(Duration::from_secs(10)),
            max_ttl: Some(Duration::from_secs(600)),
            max_active_sessions: 3,
            eviction_policy: EvictionPolicy::Lru,
            cleanup_interval: Duration::from_secs(1),
        }
    }

    #[tokio::test]
    async fn register_and_check_session() {
        let mgr = SessionWorkingSetManager::new(config_for_tests());
        assert!(mgr.register("s1").await);
        assert!(!mgr.register("s1").await); // duplicate
        assert_eq!(mgr.check_session("s1").await, Some(SessionStatus::Active));
        assert_eq!(mgr.check_session("nonexistent").await, None);
    }

    #[tokio::test]
    async fn mark_active_resets_idle() {
        let mut cfg = config_for_tests();
        cfg.idle_timeout = Some(Duration::from_millis(10));
        let mgr = SessionWorkingSetManager::new(cfg);
        mgr.register("s1").await;

        // Let idle timeout expire.
        tokio::time::sleep(Duration::from_millis(20)).await;
        mgr.run_cleanup().await;
        assert_eq!(mgr.check_session("s1").await, Some(SessionStatus::Idle));

        // Mark active and verify it recovers.
        mgr.mark_active("s1").await;
        assert_eq!(mgr.check_session("s1").await, Some(SessionStatus::Active));
    }

    #[tokio::test]
    async fn ttl_expiry() {
        let mut cfg = config_for_tests();
        cfg.idle_timeout = None;
        cfg.max_ttl = Some(Duration::from_millis(10));
        let mgr = SessionWorkingSetManager::new(cfg);
        mgr.register("s1").await;

        tokio::time::sleep(Duration::from_millis(20)).await;
        mgr.run_cleanup().await;
        assert_eq!(mgr.check_session("s1").await, Some(SessionStatus::Expired));
    }

    #[tokio::test]
    async fn eviction_on_capacity_exceeded() {
        let mut cfg = config_for_tests();
        cfg.max_active_sessions = 2;
        cfg.idle_timeout = None;
        cfg.max_ttl = None;
        let mgr = SessionWorkingSetManager::new(cfg);

        mgr.register("s1").await;
        mgr.register("s2").await;
        mgr.register("s3").await;

        assert_eq!(mgr.active_count().await, 3);
        mgr.run_cleanup().await;
        assert_eq!(mgr.active_count().await, 2);
    }

    #[tokio::test]
    async fn eviction_removes_expired_first() {
        let mut cfg = config_for_tests();
        cfg.max_active_sessions = 2;
        cfg.max_ttl = Some(Duration::from_millis(10));
        cfg.idle_timeout = None;
        let mgr = SessionWorkingSetManager::new(cfg);

        mgr.register("s1").await;
        mgr.register("s2").await;
        mgr.register("s3").await;

        tokio::time::sleep(Duration::from_millis(20)).await;
        mgr.run_cleanup().await;

        assert_eq!(mgr.active_count().await, 0); // all expired
    }

    #[tokio::test]
    async fn close_removes_session() {
        let mgr = SessionWorkingSetManager::new(config_for_tests());
        mgr.register("s1").await;
        assert!(mgr.close("s1").await);
        assert!(!mgr.close("s1").await); // already removed
        assert_eq!(mgr.check_session("s1").await, None);
    }

    #[tokio::test]
    async fn unregister_removes_session() {
        let mgr = SessionWorkingSetManager::new(config_for_tests());
        mgr.register("s1").await;
        assert!(mgr.unregister("s1").await);
        assert!(!mgr.unregister("s1").await);
    }

    #[tokio::test]
    async fn update_message_count_and_status_snapshot() {
        let mgr = SessionWorkingSetManager::new(config_for_tests());
        mgr.register("s1").await;
        mgr.register("s2").await;
        mgr.update_message_count("s1", 42).await;

        let snapshot = mgr.status_snapshot().await;
        assert_eq!(snapshot.len(), 2);
        assert!(snapshot.contains(&("s1".to_string(), SessionStatus::Active)));
        assert!(snapshot.contains(&("s2".to_string(), SessionStatus::Active)));
    }

    #[tokio::test]
    async fn idle_does_not_transition_already_closed() {
        let mut cfg = config_for_tests();
        cfg.idle_timeout = Some(Duration::from_millis(10));
        let mgr = SessionWorkingSetManager::new(cfg);
        mgr.register("s1").await;
        mgr.close("s1").await;

        tokio::time::sleep(Duration::from_millis(20)).await;
        mgr.run_cleanup().await;
        assert_eq!(mgr.check_session("s1").await, None);
    }
}
