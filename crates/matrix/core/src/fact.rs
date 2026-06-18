use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixFact {
    pub fact_id: String,
    pub snapshot_id: String,
    pub fact_type: String,
    #[serde(default)]
    pub entity_refs: Vec<String>,
    #[serde(default)]
    pub metric_key: Option<String>,
    #[serde(default)]
    pub dimensions: Value,
    #[serde(default)]
    pub measures: Value,
    pub event_time: DateTime<Utc>,
    #[serde(default)]
    pub valid_from: Option<DateTime<Utc>>,
    #[serde(default)]
    pub valid_to: Option<DateTime<Utc>>,
    #[serde(default)]
    pub source_ref: Option<String>,
    pub confidence: f32,
    pub raw_hash: String,
}

impl MatrixFact {
    #[must_use]
    pub fn from_input(input: MatrixFactInput) -> Self {
        let event_time = input.event_time.unwrap_or_else(Utc::now);
        let fact_id = input
            .fact_id
            .unwrap_or_else(|| format!("fact-{}", uuid::Uuid::new_v4()));
        let snapshot_id = input
            .snapshot_id
            .unwrap_or_else(|| format!("snapshot-inline-{}", uuid::Uuid::new_v4()));
        let raw_hash = input.raw_hash.unwrap_or_else(|| {
            stable_hash(&serde_json::json!({
                "snapshot_id": snapshot_id,
                "fact_type": input.fact_type,
                "entity_refs": input.entity_refs,
                "metric_key": input.metric_key,
                "dimensions": input.dimensions,
                "measures": input.measures,
                "event_time": event_time,
                "source_ref": input.source_ref,
            }))
        });
        Self {
            fact_id,
            snapshot_id,
            fact_type: input.fact_type,
            entity_refs: input.entity_refs,
            metric_key: input.metric_key,
            dimensions: input.dimensions,
            measures: input.measures,
            event_time,
            valid_from: input.valid_from,
            valid_to: input.valid_to,
            source_ref: input.source_ref,
            confidence: input.confidence.unwrap_or(1.0),
            raw_hash,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixFactInput {
    #[serde(default)]
    pub fact_id: Option<String>,
    #[serde(default)]
    pub snapshot_id: Option<String>,
    pub fact_type: String,
    #[serde(default)]
    pub entity_refs: Vec<String>,
    #[serde(default)]
    pub metric_key: Option<String>,
    #[serde(default)]
    pub dimensions: Value,
    #[serde(default)]
    pub measures: Value,
    #[serde(default)]
    pub event_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub valid_from: Option<DateTime<Utc>>,
    #[serde(default)]
    pub valid_to: Option<DateTime<Utc>>,
    #[serde(default)]
    pub source_ref: Option<String>,
    #[serde(default)]
    pub confidence: Option<f32>,
    #[serde(default)]
    pub raw_hash: Option<String>,
}

fn stable_hash(value: &Value) -> String {
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    let digest = Sha256::digest(bytes);
    format!("sha256:{digest:x}")
}
