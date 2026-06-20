use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactReality {
    Observed,
    Inferred,
    Simulated,
    Hypothetical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HypothesisBoundary {
    pub reality: FactReality,
    pub scenario_id: Option<String>,
    pub promotion_allowed: bool,
}

impl HypothesisBoundary {
    #[must_use]
    pub fn observed() -> Self {
        Self {
            reality: FactReality::Observed,
            scenario_id: None,
            promotion_allowed: true,
        }
    }

    #[must_use]
    pub fn hypothetical(scenario_id: impl Into<String>) -> Self {
        Self {
            reality: FactReality::Hypothetical,
            scenario_id: Some(scenario_id.into()),
            promotion_allowed: false,
        }
    }
}
