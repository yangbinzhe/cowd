use std::collections::BTreeMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use harness_contract::agent::{
    AgentDefinitionRevisionRef, AgentEvaluationContract, DefinitionScope,
};

/// Workspace-scoped, long-lived agent metadata. It intentionally excludes
/// lifecycle state, which is reconstructed from AgentRuntime events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCatalogEntry {
    /// Exact immutable Definition selected for this reusable identity.
    pub definition_ref: AgentDefinitionRevisionRef,
    pub agent_id: String,
    pub name: String,
    pub description: String,
    pub capabilities: Vec<String>,
    pub scope: DefinitionScope,
    pub evaluation: AgentEvaluationContract,
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

    /// Replace the catalog from the Runtime Definition projection. This is
    /// intentionally all-or-nothing so stale runnable entries cannot survive
    /// a release stop, revoke, or restart.
    pub fn replace_all(&self, entries: Vec<AgentCatalogEntry>) {
        let entries = entries
            .into_iter()
            .map(|entry| (entry.agent_id.clone(), entry))
            .collect();
        *self
            .entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = entries;
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
    pub fn all(&self) -> Vec<AgentCatalogEntry> {
        let mut entries = self
            .entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));
        entries
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
        entries.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));
        entries
    }

    /// Search only the Runtime-built Definition catalog. This is an
    /// explanatory candidate ranking for callers; it neither scans files nor
    /// chooses a Team execution plan.
    #[must_use]
    pub fn search(&self, query: &str) -> Vec<AgentCatalogEntry> {
        let terms = query
            .split(|character: char| !character.is_alphanumeric())
            .map(str::trim)
            .filter(|term| !term.is_empty())
            .map(|term| term.to_ascii_lowercase())
            .collect::<Vec<_>>();
        let mut ranked = self
            .all()
            .into_iter()
            .filter_map(|entry| {
                let haystack = format!(
                    "{} {} {} {}",
                    entry.agent_id,
                    entry.name,
                    entry.description,
                    entry.capabilities.join(" ")
                )
                .to_ascii_lowercase();
                let score = terms
                    .iter()
                    .filter(|term| haystack.contains(term.as_str()))
                    .count();
                (terms.is_empty() || score > 0).then_some((score, entry))
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|(left_score, left), (right_score, right)| {
            right_score
                .cmp(left_score)
                .then_with(|| left.agent_id.cmp(&right.agent_id))
        });
        ranked.into_iter().map(|(_, entry)| entry).collect()
    }
}
