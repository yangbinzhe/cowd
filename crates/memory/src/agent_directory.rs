//! AgentDirectory — sub-agent mutual discovery registry.
//!
//! Provides a global, thread-safe registry where sub-agents can discover
//! each other and query "who can help with X" based on declared capabilities.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::OnceLock;

static DIRECTORY: OnceLock<AgentDirectory> = OnceLock::new();

/// Quality / reputation score accumulated by an agent across tasks.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReputationScore {
    /// Success rate (0.0 – 1.0) over all completed tasks.
    pub success_rate: f64,
    /// Total number of tasks this agent has participated in.
    pub task_count: u64,
    /// Peer-assigned rating (0.0 – 5.0).
    pub peer_rating: f64,
    /// Timestamp (ms since epoch) of the most recent successful completion.
    pub last_success_at_ms: u64,
    /// Number of consecutive failures since the last success (resets on success).
    pub recent_failures: u32,
}

impl ReputationScore {
    /// Composite score combining success rate, peer rating, and recency.
    /// Returns a value in [0.0, 10.0] suitable for ranking.
    pub fn composite(&self) -> f64 {
        let success_component = self.success_rate * 4.0;
        let peer_component = (self.peer_rating / 5.0) * 3.0;
        // Recency bonus — max 3.0 for successes within the last hour, decays over 24h
        let now_ms = current_time_millis();
        let recency = if self.last_success_at_ms > 0 {
            let age_secs = (now_ms.saturating_sub(self.last_success_at_ms) as f64) / 1000.0;
            if age_secs < 3_600.0 {
                3.0
            } else if age_secs < 86_400.0 {
                3.0 * (1.0 - (age_secs - 3_600.0) / (86_400.0 - 3_600.0))
            } else {
                0.0
            }
        } else {
            0.0
        };
        // Penalty for recent failures
        let failure_penalty = (self.recent_failures as f64 * 0.5).min(2.0);
        (success_component + peer_component + recency - failure_penalty).max(0.0)
    }
}

/// Metadata for a single registered agent.
#[derive(Debug, Clone)]
pub struct AgentInfo {
    /// Unique identifier for this agent instance.
    pub agent_id: String,
    /// Human-readable role (e.g. "Planner", "Executor", "Reviewer").
    pub role: String,
    /// Declared capabilities / skills (e.g. ["rust", "refactoring", "testing"]).
    pub capabilities: Vec<String>,
    /// Current operational status.
    pub status: AgentStatus,
    /// Timestamp (ms since epoch) when the agent registered.
    pub registered_at_ms: u64,
    /// Timestamp (ms since epoch) of the last status update / heartbeat.
    pub last_heartbeat_ms: u64,
    /// Accumulated reputation score (optional — populated by TeamDiscovery).
    pub reputation: Option<ReputationScore>,
}

/// Operational status of an agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentStatus {
    Active,
    Idle,
    Busy,
    Offline,
}

/// Thread-safe global registry of active sub-agents.
///
/// Use `AgentDirectory::global()` to access the singleton.
pub struct AgentDirectory {
    agents: Mutex<HashMap<String, AgentInfo>>,
}

impl AgentDirectory {
    /// Obtain a reference to the global singleton directory.
    pub fn global() -> &'static Self {
        DIRECTORY.get_or_init(|| Self {
            agents: Mutex::new(HashMap::new()),
        })
    }

    /// Register a new agent in the directory.
    pub fn register(&self, info: AgentInfo) {
        self.agents.lock().insert(info.agent_id.clone(), info);
    }

    /// Remove an agent from the directory (e.g. on completion / shutdown).
    pub fn unregister(&self, agent_id: &str) {
        self.agents.lock().remove(agent_id);
    }

    /// Update the status and heartbeat timestamp of an agent.
    pub fn update_status(&self, agent_id: &str, status: AgentStatus) {
        let mut lock = self.agents.lock();
        if let Some(info) = lock.get_mut(agent_id) {
            info.status = status;
            info.last_heartbeat_ms = current_time_millis();
        }
    }

    /// Update the reputation score of a registered agent.
    ///
    /// This is the bridge between ReputationManager (persistent SQLite storage)
    /// and TeamDiscovery (which reads AgentInfo.reputation for ranking).
    /// Called after each `record_completion()` in agent.rs.
    ///
    /// If the agent_id is not registered, this is a no-op.
    pub fn update_reputation(&self, agent_id: &str, rep: ReputationScore) {
        let mut lock = self.agents.lock();
        if let Some(info) = lock.get_mut(agent_id) {
            info.reputation = Some(rep);
        }
    }

    /// Discover agents that match at least one of the required capabilities.
    ///
    /// Offline agents are excluded from results.
    pub fn discover(&self, required_capabilities: &[String]) -> Vec<AgentInfo> {
        self.agents
            .lock()
            .values()
            .filter(|a| a.status != AgentStatus::Offline)
            .filter(|a| {
                required_capabilities
                    .iter()
                    .any(|c| a.capabilities.contains(c))
            })
            .cloned()
            .collect()
    }

    /// List all agents that are not offline.
    pub fn list_active(&self) -> Vec<AgentInfo> {
        self.agents
            .lock()
            .values()
            .filter(|a| a.status != AgentStatus::Offline)
            .cloned()
            .collect()
    }

    /// Clear all agents from the directory (mainly for testing).
    #[doc(hidden)]
    pub fn clear_all(&self) {
        self.agents.lock().clear();
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Current wall-clock time in milliseconds since the Unix epoch.
fn current_time_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_agent(id: &str, capabilities: Vec<String>) -> AgentInfo {
        AgentInfo {
            agent_id: id.to_string(),
            role: "Executor".to_string(),
            capabilities,
            status: AgentStatus::Active,
            registered_at_ms: current_time_millis(),
            last_heartbeat_ms: current_time_millis(),
            reputation: None,
        }
    }

    #[test]
    fn register_and_lookup() {
        let dir = AgentDirectory::global();
        dir.clear_all(); // ensure clean state
        let info = dummy_agent("test-1", vec!["rust".to_string()]);
        dir.register(info);
        let active = dir.list_active();
        assert!(active.iter().any(|a| a.agent_id == "test-1"));
        dir.unregister("test-1");
    }

    #[test]
    fn discover_by_capability() {
        let dir = AgentDirectory::global();
        dir.clear_all(); // ensure clean state
        dir.register(dummy_agent(
            "a1",
            vec!["rust".to_string(), "testing".to_string()],
        ));
        dir.register(dummy_agent("a2", vec!["python".to_string()]));

        let rust_agents = dir.discover(&["rust".to_string()]);
        assert_eq!(rust_agents.len(), 1);
        assert_eq!(rust_agents[0].agent_id, "a1");

        // Cleanup
        dir.unregister("a1");
        dir.unregister("a2");
    }

    #[test]
    fn offline_agents_are_excluded() {
        let dir = AgentDirectory::global();
        dir.clear_all(); // ensure clean state
        let mut offline = dummy_agent("off-1", vec!["rust".to_string()]);
        offline.status = AgentStatus::Offline;
        dir.register(offline);

        let active = dir.discover(&["rust".to_string()]);
        assert!(active.is_empty());

        dir.unregister("off-1");
    }

    #[test]
    fn test_update_reputation_and_read_back() {
        let dir = AgentDirectory::global();
        dir.clear_all();
        let info = dummy_agent("rep-1", vec!["rust".to_string()]);
        dir.register(info);

        dir.update_reputation(
            "rep-1",
            ReputationScore {
                success_rate: 0.85,
                task_count: 10,
                peer_rating: 4.0,
                last_success_at_ms: current_time_millis(),
                recent_failures: 0,
            },
        );

        let active = dir.list_active();
        let agent = active.iter().find(|a| a.agent_id == "rep-1").unwrap();
        assert_eq!(agent.reputation.unwrap().success_rate, 0.85);
        assert_eq!(agent.reputation.unwrap().task_count, 10);

        dir.unregister("rep-1");
    }

    #[test]
    fn test_update_reputation_noop_on_missing() {
        let dir = AgentDirectory::global();
        dir.update_reputation("ghost", ReputationScore::default());
        let active = dir.list_active();
        assert!(active.iter().find(|a| a.agent_id == "ghost").is_none());
    }

    #[test]
    fn update_status_preserves_other_fields() {
        let dir = AgentDirectory::global();
        dir.clear_all();
        let info = dummy_agent("st-1", vec!["go".to_string()]);
        let orig_registered = info.registered_at_ms;
        dir.register(info);

        dir.update_status("st-1", AgentStatus::Busy);
        let active = dir.list_active();
        let found = active.iter().find(|a| a.agent_id == "st-1").unwrap();
        assert_eq!(found.status, AgentStatus::Busy);
        assert_eq!(found.registered_at_ms, orig_registered);
        assert!(found.last_heartbeat_ms >= orig_registered);

        dir.unregister("st-1");
    }
}
