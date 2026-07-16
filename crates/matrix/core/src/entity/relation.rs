use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::MatrixEntity;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixRelationInput {
    #[serde(default)]
    pub relation_id: Option<String>,
    pub relation_type: String,
    pub from_entity_id: String,
    pub to_entity_id: String,
    #[serde(default)]
    pub attributes: Value,
    #[serde(default)]
    pub confidence: Option<f32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixRelation {
    pub relation_id: String,
    pub relation_type: String,
    pub from_entity_id: String,
    pub to_entity_id: String,
    #[serde(default)]
    pub attributes: Value,
    pub confidence: f32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl MatrixRelation {
    #[must_use]
    pub fn from_input(input: MatrixRelationInput) -> Self {
        let now = Utc::now();
        Self {
            relation_id: input
                .relation_id
                .unwrap_or_else(|| format!("relation-{}", uuid::Uuid::new_v4())),
            relation_type: input.relation_type.trim().to_ascii_lowercase(),
            from_entity_id: input.from_entity_id,
            to_entity_id: input.to_entity_id,
            attributes: input.attributes,
            confidence: input.confidence.unwrap_or(1.0),
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixImpactHop {
    pub depth: usize,
    pub traversal_direction: String,
    pub relation: MatrixRelation,
    #[serde(default)]
    pub from_entity: Option<MatrixEntity>,
    #[serde(default)]
    pub to_entity: Option<MatrixEntity>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixImpactTrace {
    pub root_entity_id: String,
    pub max_depth: usize,
    #[serde(default)]
    pub entities: Vec<MatrixEntity>,
    #[serde(default)]
    pub hops: Vec<MatrixImpactHop>,
    pub generated_at: DateTime<Utc>,
}
