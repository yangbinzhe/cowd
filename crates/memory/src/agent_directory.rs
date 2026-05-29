//! AgentDirectory — sub-agent mutual discovery registry.
//!
//! Provides a global, thread-safe registry where sub-agents can discover
//! each other and query "who can help with X" based on declared capabilities.

use std::collections::HashMap;
use parking_lot::Mutex;
use std::sync::OnceLock;

static DIRECTORY: OnceLock<AgentDirectory> = OnceLock::new();

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
        }
    }

    #[test]
    fn register_and_lookup() {
        // NOTE: global singleton is shared across tests — clear after.
        let dir = AgentDirectory::global();
        let info = dummy_agent("test-1", vec!["rust".to_string()]);
        dir.register(info);
        let active = dir.list_active();
        assert!(active.iter().any(|a| a.agent_id == "test-1"));
        dir.unregister("test-1");
    }

    #[test]
    fn discover_by_capability() {
        let dir = AgentDirectory::global();
        dir.register(dummy_agent("a1", vec!["rust".to_string(), "testing".to_string()]));
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
        let mut offline = dummy_agent("off-1", vec!["rust".to_string()]);
        offline.status = AgentStatus::Offline;
        dir.register(offline);

        let active = dir.discover(&["rust".to_string()]);
        assert!(active.is_empty());

        dir.unregister("off-1");
    }

    #[test]
    fn update_status_preserves_other_fields() {
        let dir = AgentDirectory::global();
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
