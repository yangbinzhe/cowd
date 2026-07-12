use std::path::Path;

use harness_contract::goal::{RuntimeIntervention, RuntimeInterventionKind};

use super::{
    EvolutionSignal, EvolutionSignalInput, EvolutionSignalSeverity, EvolutionSignalSource,
    EvolutionSignalStore, EvolutionSignalType,
};

#[derive(Debug, Clone)]
pub struct EvolutionSignalCollector {
    store: EvolutionSignalStore,
}

impl EvolutionSignalCollector {
    #[must_use]
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            store: EvolutionSignalStore::new(root),
        }
    }

    #[must_use]
    pub fn default_for_config_home(config_home: impl AsRef<Path>) -> Self {
        Self::new(config_home.as_ref().join("evolution"))
    }

    pub fn append_intervention_signal(
        &self,
        session_id: impl Into<String>,
        intervention: &RuntimeIntervention,
    ) -> Result<Option<EvolutionSignal>, String> {
        let Some(signal) = signal_from_intervention(session_id.into(), intervention) else {
            return Ok(None);
        };
        self.store.append(&signal)?;
        Ok(Some(signal))
    }

    #[must_use]
    pub fn store(&self) -> &EvolutionSignalStore {
        &self.store
    }
}

#[must_use]
pub fn signal_from_intervention(
    session_id: String,
    intervention: &RuntimeIntervention,
) -> Option<EvolutionSignal> {
    let (signal_type, severity, immediate_task_can_continue, suggested_action) =
        match intervention.kind {
            RuntimeInterventionKind::Continue
            | RuntimeInterventionKind::Parallelize
            | RuntimeInterventionKind::Synthesize => return None,
            RuntimeInterventionKind::Retrieve => (
                EvolutionSignalType::LowNoveltyToolLoop,
                EvolutionSignalSeverity::Warning,
                true,
                "Improve evidence reuse, batching, or scoped retrieval before another tool call",
            ),
            RuntimeInterventionKind::Replan | RuntimeInterventionKind::Switch => (
                EvolutionSignalType::SlowProgress,
                EvolutionSignalSeverity::Warning,
                true,
                "Inspect strategy selection, provider affordances, and graph replan evidence",
            ),
            RuntimeInterventionKind::Block => (
                EvolutionSignalType::SlowProgress,
                EvolutionSignalSeverity::Critical,
                false,
                "Create a governed recovery or capability-improvement proposal from the checked blockers",
            ),
        };

    Some(EvolutionSignal::new(EvolutionSignalInput {
        signal_type,
        source: EvolutionSignalSource {
            owner: "runtime.execution_core.goal".to_string(),
            session_id: Some(session_id),
            agent_id: None,
            team_id: None,
            run_id: None,
        },
        evidence_refs: intervention.evidence_refs.clone(),
        severity,
        summary: intervention.reason.clone(),
        suggested_action: suggested_action.to_string(),
        immediate_task_can_continue,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retrieval_intervention_becomes_governed_evolution_signal() {
        let signal = signal_from_intervention(
            "session-1".to_string(),
            &RuntimeIntervention {
                goal_id: "goal-1".to_string(),
                kind: RuntimeInterventionKind::Retrieve,
                reason: "low novelty".to_string(),
                evidence_refs: vec!["tool:read_file:README.md".to_string()],
                expected_graph_revision: None,
            },
        )
        .expect("signal");

        assert_eq!(signal.signal_type, EvolutionSignalType::LowNoveltyToolLoop);
        assert_eq!(signal.source.owner, "runtime.execution_core.goal");
        assert!(signal.immediate_task_can_continue);
    }
}
