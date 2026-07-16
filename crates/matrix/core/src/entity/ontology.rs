use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::MatrixEntity;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixOntologyConcept {
    pub concept_id: String,
    pub name: String,
    pub domain: String,
    pub entity_type: String,
    #[serde(default)]
    pub required_attributes: Vec<String>,
    #[serde(default)]
    pub source_systems: Vec<String>,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixOntologyRelation {
    pub relation_type: String,
    pub from_concept_id: String,
    pub to_concept_id: String,
    pub cardinality: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixOntologyMetricBinding {
    pub metric_id: String,
    pub concept_id: String,
    pub grain: String,
    pub semantic_role: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixOntologyPack {
    pub ontology_id: String,
    pub domain: String,
    pub version: String,
    #[serde(default)]
    pub concepts: Vec<MatrixOntologyConcept>,
    #[serde(default)]
    pub relations: Vec<MatrixOntologyRelation>,
    #[serde(default)]
    pub metric_bindings: Vec<MatrixOntologyMetricBinding>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixEntityMatchCandidate {
    pub candidate_id: String,
    pub left_entity_id: String,
    pub right_entity_id: String,
    pub match_type: String,
    pub confidence: f32,
    #[serde(default)]
    pub reason_codes: Vec<String>,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixEntityConflictDecision {
    pub decision_id: String,
    pub candidate_id: String,
    pub decision: String,
    pub survivor_entity_id: String,
    pub retired_entity_id: String,
    pub survivorship_rule: String,
    #[serde(default)]
    pub notes: Option<String>,
    #[serde(default)]
    pub decision_metadata: Value,
    pub decided_at: DateTime<Utc>,
}

#[must_use]
pub fn match_candidate(
    left: &MatrixEntity,
    right: &MatrixEntity,
) -> Option<MatrixEntityMatchCandidate> {
    if left.entity_id == right.entity_id || left.entity_type != right.entity_type {
        return None;
    }
    let mut confidence: f32 = 0.0;
    let mut reason_codes = Vec::new();
    if left.canonical_key == right.canonical_key {
        confidence += 0.55;
        reason_codes.push("same_canonical_key".to_string());
    }
    if !left
        .source_keys
        .iter()
        .filter(|left_key| {
            right.source_keys.iter().any(|right_key| {
                left_key.normalized_system() == right_key.normalized_system()
                    && left_key.normalized_key() == right_key.normalized_key()
            })
        })
        .collect::<Vec<_>>()
        .is_empty()
    {
        confidence += 0.35;
        reason_codes.push("same_source_key".to_string());
    }
    if comparable_text(&left.display_name) == comparable_text(&right.display_name) {
        confidence += 0.5;
        reason_codes.push("same_display_name".to_string());
    }
    if confidence < 0.5 {
        return None;
    }
    Some(MatrixEntityMatchCandidate {
        candidate_id: format!("entity-match-{}", uuid::Uuid::new_v4()),
        left_entity_id: left.entity_id.clone(),
        right_entity_id: right.entity_id.clone(),
        match_type: "possible_duplicate".to_string(),
        confidence: confidence.min(1.0),
        reason_codes,
        status: "open".to_string(),
        created_at: Utc::now(),
    })
}

fn comparable_text(value: &str) -> String {
    value
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .flat_map(char::to_lowercase)
        .collect()
}
