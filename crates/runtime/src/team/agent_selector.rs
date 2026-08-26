use std::sync::Arc;

use crate::{AgentCatalog, AgentCatalogEntry, AgentRuntime};

/// Selects a reusable identity from the workspace catalog. It never starts an
/// Agent; graph execution and `AgentRuntime` own that transition.
pub struct AgentSelector {
    catalog: Arc<AgentCatalog>,
    runtime: Arc<AgentRuntime>,
}

impl AgentSelector {
    #[must_use]
    pub fn new(catalog: Arc<AgentCatalog>, runtime: Arc<AgentRuntime>) -> Self {
        Self { catalog, runtime }
    }

    #[must_use]
    pub fn select(&self, capabilities: &[String]) -> Option<AgentCatalogEntry> {
        self.catalog
            .discover(capabilities)
            .into_iter()
            .find(|entry| {
                self.runtime
                    .get(&entry.agent_id)
                    .is_none_or(|run| run.status.is_terminal())
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AgentCatalogEntry, ProviderRegistry, RuntimeEventStore};
    use harness_contract::agent::{
        AgentDefinitionId, AgentDefinitionRevisionRef, AgentEvaluationContract, DefinitionScope,
    };

    #[test]
    fn selects_catalog_identity_without_creating_a_second_lifecycle() {
        let runtime = Arc::new(AgentRuntime::new(
            Arc::new(RuntimeEventStore::try_open_in_memory().unwrap()),
            Arc::new(ProviderRegistry::empty()),
        ));
        let catalog = Arc::new(AgentCatalog::new());
        catalog.upsert(AgentCatalogEntry {
            definition_ref: AgentDefinitionRevisionRef::new(
                AgentDefinitionId::new(DefinitionScope::Workspace, "reviewer").unwrap(),
                1,
            )
            .unwrap(),
            agent_id: "reviewer".into(),
            name: "Reviewer".into(),
            description: "Reviews evidence".into(),
            capabilities: vec!["review".into()],
            skill_refs: Vec::new(),
            scope: DefinitionScope::Workspace,
            evaluation: AgentEvaluationContract::single_release_gate("review", "evidence"),
        });
        let selected = AgentSelector::new(catalog, runtime).select(&["review".into()]);
        assert_eq!(selected.unwrap().agent_id, "reviewer");
    }
}
