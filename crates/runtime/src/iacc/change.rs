use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IaccChangeEvent {
    pub change_id: String,
    pub change_type: String,
    pub entity_ref: String,
    #[serde(default)]
    pub metric_id: Option<String>,
    #[serde(default)]
    pub from_value: Option<Value>,
    #[serde(default)]
    pub to_value: Option<Value>,
    pub delta: f64,
    pub period: String,
    pub detected_at: DateTime<Utc>,
    #[serde(default)]
    pub source_fact_refs: Vec<String>,
    pub severity_hint: String,
}

impl IaccChangeEvent {
    #[must_use]
    pub fn severity_for_delta(delta: f64) -> String {
        if delta.abs() >= 100.0 {
            "critical".to_string()
        } else if delta.abs() > 0.0 {
            "warning".to_string()
        } else {
            "normal".to_string()
        }
    }
}
