use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionMemoryScope {
    pub scope_id: String,
    pub goal_ids: Vec<String>,
    pub owner: String,
    pub activation_policy: Vec<String>,
}

impl EvolutionMemoryScope {
    #[must_use]
    pub fn for_goals(owner: impl Into<String>, goal_ids: Vec<String>) -> Self {
        Self {
            scope_id: "evolution".to_string(),
            goal_ids,
            owner: owner.into(),
            activation_policy: vec![
                "system_capability_improvement".to_string(),
                "same_failure_pattern".to_string(),
                "explicit_evolution_analysis".to_string(),
            ],
        }
    }
}
