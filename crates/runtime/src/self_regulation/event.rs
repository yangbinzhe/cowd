use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{RuntimeAdaptiveDecision, ToolProgressObservation};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeSelfRegulationEventInput {
    pub session_id: String,
    pub sequence: usize,
    pub observation: Value,
    pub decision: RuntimeAdaptiveDecision,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeSelfRegulationEvent {
    pub event_type: String,
    pub source: String,
    pub status: String,
    pub payload: Value,
}

impl RuntimeSelfRegulationEvent {
    #[must_use]
    pub fn from_tool_decision(
        observation: &ToolProgressObservation,
        decision: &RuntimeAdaptiveDecision,
    ) -> Self {
        let fingerprint = observation.fingerprint();
        Self {
            event_type: "runtime.self_regulation.decision".to_string(),
            source: "runtime.self_regulation".to_string(),
            status: decision.kind_str().to_string(),
            payload: json!({
                "decision": decision.kind_str(),
                "reason": decision.reason(),
                "reason_code": decision.reason_code(),
                "recommended_action": decision.recommended_action(),
                "recommended_pattern": decision.recommended_pattern().map(|mode| format!("{mode:?}")),
                "tool": {
                    "name": fingerprint.tool_name,
                    "target": fingerprint.target,
                    "range": fingerprint.range,
                    "input_hash": fingerprint.input_hash,
                    "output_hash": fingerprint.output_hash,
                    "is_error": observation.is_error(),
                },
                "prompt_injected": decision.prompt(),
                "source": "runtime.self_regulation",
            }),
        }
    }
}
