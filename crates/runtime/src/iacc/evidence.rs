use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IaccEvidenceSourceRef {
    pub kind: String,
    pub reference: String,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IaccEvidencePacket {
    pub packet_id: String,
    #[serde(default)]
    pub attention_id: Option<String>,
    pub problem_statement: String,
    #[serde(default)]
    pub business_context: Value,
    #[serde(default)]
    pub metric_evidence: Vec<Value>,
    #[serde(default)]
    pub change_evidence: Vec<Value>,
    #[serde(default)]
    pub anomaly_evidence: Vec<Value>,
    #[serde(default)]
    pub attribution_candidates: Vec<Value>,
    #[serde(default)]
    pub impact_paths: Vec<Value>,
    #[serde(default)]
    pub source_refs: Vec<IaccEvidenceSourceRef>,
    #[serde(default)]
    pub missing_evidence: Vec<String>,
    pub confidence: f32,
    pub token_budget: u64,
    pub created_at: DateTime<Utc>,
}

impl IaccEvidencePacket {
    #[must_use]
    pub fn new(problem_statement: impl Into<String>) -> Self {
        Self {
            packet_id: format!("evidence-{}", uuid::Uuid::new_v4()),
            attention_id: None,
            problem_statement: problem_statement.into(),
            business_context: Value::Null,
            metric_evidence: Vec::new(),
            change_evidence: Vec::new(),
            anomaly_evidence: Vec::new(),
            attribution_candidates: Vec::new(),
            impact_paths: Vec::new(),
            source_refs: Vec::new(),
            missing_evidence: vec![
                "metric_network_not_computed_in_v0.9.77".to_string(),
                "attribution_not_computed_in_v0.9.77".to_string(),
                "impact_paths_not_computed_in_v0.9.77".to_string(),
            ],
            confidence: 0.3,
            token_budget: 4_000,
            created_at: Utc::now(),
        }
    }
}
