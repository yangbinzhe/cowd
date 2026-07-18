use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixSourceKey {
    pub source_system: String,
    pub source_key: String,
    #[serde(default)]
    pub source_ref: Option<String>,
}

impl MatrixSourceKey {
    #[must_use]
    pub fn normalized_system(&self) -> String {
        normalize_key(&self.source_system)
    }

    #[must_use]
    pub fn normalized_key(&self) -> String {
        normalize_key(&self.source_key)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixEntityInput {
    #[serde(default)]
    pub entity_id: Option<String>,
    pub entity_type: String,
    pub canonical_key: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub source_keys: Vec<MatrixSourceKey>,
    #[serde(default)]
    pub attributes: Value,
    #[serde(default)]
    pub confidence: Option<f32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixEntity {
    pub entity_id: String,
    pub entity_type: String,
    pub canonical_key: String,
    pub display_name: String,
    #[serde(default)]
    pub source_keys: Vec<MatrixSourceKey>,
    #[serde(default)]
    pub attributes: Value,
    pub confidence: f32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl MatrixEntity {
    #[must_use]
    pub fn from_input(input: MatrixEntityInput) -> Self {
        let now = Utc::now();
        let canonical_key = normalize_key(&input.canonical_key);
        let display_name = input
            .display_name
            .unwrap_or_else(|| input.canonical_key.clone());
        Self {
            entity_id: input
                .entity_id
                .unwrap_or_else(|| format!("entity-{}", uuid::Uuid::new_v4())),
            entity_type: normalize_key(&input.entity_type),
            canonical_key,
            display_name,
            source_keys: input.source_keys,
            attributes: input.attributes,
            confidence: input.confidence.unwrap_or(1.0),
            created_at: now,
            updated_at: now,
        }
    }

    #[must_use]
    pub fn reference(&self) -> String {
        format!("matrix:entity:{}", self.entity_id)
    }
}

#[must_use]
pub fn normalize_key(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace(' ', "_")
}
