pub use harness_contract::reality::RealityBoundary;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HypothesisBoundary {
    pub reality: RealityBoundary,
    pub scenario_id: Option<String>,
    pub promotion_allowed: bool,
}

impl HypothesisBoundary {
    #[must_use]
    pub fn observed() -> Self {
        Self {
            reality: RealityBoundary::Observed,
            scenario_id: None,
            promotion_allowed: true,
        }
    }

    #[must_use]
    pub fn hypothetical(scenario_id: impl Into<String>) -> Self {
        Self {
            reality: RealityBoundary::Hypothetical,
            scenario_id: Some(scenario_id.into()),
            promotion_allowed: false,
        }
    }
}
