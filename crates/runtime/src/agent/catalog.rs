use std::collections::BTreeMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

/// Workspace-scoped, long-lived agent metadata. It intentionally excludes
/// lifecycle state, which is reconstructed from AgentRuntime events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCatalogEntry {
    pub agent_id: String,
    pub capabilities: Vec<String>,
    pub reputation: i32,
}

#[derive(Debug, Default)]
pub struct AgentCatalog {
    entries: RwLock<BTreeMap<String, AgentCatalogEntry>>,
}

impl AgentCatalog {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert(&self, entry: AgentCatalogEntry) {
        self.entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(entry.agent_id.clone(), entry);
    }

    #[must_use]
    pub fn get(&self, agent_id: &str) -> Option<AgentCatalogEntry> {
        self.entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(agent_id)
            .cloned()
    }

    #[must_use]
    pub fn discover(&self, capabilities: &[String]) -> Vec<AgentCatalogEntry> {
        let mut entries = self
            .entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter(|entry| {
                capabilities
                    .iter()
                    .all(|required| entry.capabilities.contains(required))
            })
            .cloned()
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| {
            right
                .reputation
                .cmp(&left.reputation)
                .then_with(|| left.agent_id.cmp(&right.agent_id))
        });
        entries
    }
}
