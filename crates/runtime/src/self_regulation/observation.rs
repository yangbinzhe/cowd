use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeObservationKind {
    ToolProgress,
    ContextPressure,
    ModelError,
    Recovery,
    EvolutionSignal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeStepObservation {
    pub kind: RuntimeObservationKind,
    pub source: String,
    pub summary: String,
    pub evidence_ref: Option<String>,
    #[serde(default)]
    pub payload: Value,
}

impl RuntimeStepObservation {
    #[must_use]
    pub fn new(
        kind: RuntimeObservationKind,
        source: impl Into<String>,
        summary: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            source: source.into(),
            summary: summary.into(),
            evidence_ref: None,
            payload: Value::Null,
        }
    }

    #[must_use]
    pub fn with_payload(mut self, payload: Value) -> Self {
        self.payload = payload;
        self
    }
}
