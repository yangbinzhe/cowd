use serde::{Deserialize, Serialize};

use super::{RuntimeAdaptiveDecision, RuntimeObservationKind, RuntimeStepObservation};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RuntimeProgressLedger {
    pub observations: Vec<RuntimeStepObservation>,
    pub decisions: Vec<RuntimeAdaptiveDecision>,
}

impl RuntimeProgressLedger {
    pub fn push_observation(&mut self, observation: RuntimeStepObservation) {
        self.observations.push(observation);
    }

    pub fn push_decision(&mut self, decision: RuntimeAdaptiveDecision) {
        if !matches!(decision.kind, super::RuntimeAdaptiveDecisionKind::Continue) {
            self.decisions.push(decision);
        }
    }

    #[must_use]
    pub fn compact_summary(&self) -> String {
        let tool_observations = self
            .observations
            .iter()
            .filter(|observation| observation.kind == RuntimeObservationKind::ToolProgress)
            .count();
        let context_observations = self
            .observations
            .iter()
            .filter(|observation| observation.kind == RuntimeObservationKind::ContextPressure)
            .count();
        format!(
            "observations={}, tool={}, context={}, decisions={}",
            self.observations.len(),
            tool_observations,
            context_observations,
            self.decisions.len()
        )
    }
}
