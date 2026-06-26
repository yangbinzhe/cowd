use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixSourcePack {
    pub source_pack_id: String,
    pub source_name: String,
    pub owner: String,
    pub access_mode: String,
    pub refresh_mode: String,
    #[serde(default)]
    pub entity_mappings: Vec<MatrixSourceEntityMapping>,
    #[serde(default)]
    pub fact_mappings: Vec<MatrixSourceFactMapping>,
    #[serde(default)]
    pub reconciliation_rules: Vec<String>,
    #[serde(default)]
    pub quality_rules: Vec<String>,
    #[serde(default)]
    pub freshness_sla: Option<String>,
    #[serde(default)]
    pub security_policy: Option<String>,
    #[serde(default)]
    pub metadata: Value,
    #[serde(default = "unix_epoch")]
    pub created_at: DateTime<Utc>,
    #[serde(default = "unix_epoch")]
    pub updated_at: DateTime<Utc>,
}

fn unix_epoch() -> DateTime<Utc> {
    DateTime::<Utc>::UNIX_EPOCH
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixSourceEntityMapping {
    pub source_entity: String,
    pub matrix_entity_type: String,
    pub source_key_field: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixSourceFactMapping {
    pub source_table: String,
    pub fact_type: String,
    pub metric_key: String,
    #[serde(default)]
    pub entity_ref_fields: Vec<String>,
    #[serde(default)]
    pub measure_fields: Vec<String>,
    pub dedup_key: String,
    pub delta_signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixSourcePackValidation {
    pub source_pack_id: String,
    pub status: String,
    #[serde(default)]
    pub blockers: Vec<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
    pub validated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixSourceDeltaPlan {
    pub source_pack_id: String,
    #[serde(default)]
    pub fact_types: Vec<String>,
    #[serde(default)]
    pub affected_metric_ids: Vec<String>,
    pub compute_scope: String,
    pub planned_at: DateTime<Utc>,
}

impl MatrixSourcePack {
    #[must_use]
    pub fn normalized(mut self) -> Self {
        let now = Utc::now();
        if self.source_pack_id.trim().is_empty() {
            self.source_pack_id = format!("source-pack-{}", uuid::Uuid::new_v4());
        }
        if self.created_at.timestamp() == 0 {
            self.created_at = now;
        }
        self.updated_at = now;
        self
    }

    #[must_use]
    pub fn validate(&self) -> MatrixSourcePackValidation {
        let mut blockers = Vec::new();
        let mut warnings = Vec::new();
        if self.source_name.trim().is_empty() {
            blockers.push("source_name_required".to_string());
        }
        if self.owner.trim().is_empty() {
            blockers.push("owner_required".to_string());
        }
        if self.access_mode.trim().is_empty() {
            blockers.push("access_mode_required".to_string());
        }
        if self.fact_mappings.is_empty() {
            blockers.push("fact_mapping_required".to_string());
        }
        if self.entity_mappings.is_empty() {
            warnings.push("entity_mapping_missing".to_string());
        }
        if self.reconciliation_rules.is_empty() {
            warnings.push("reconciliation_rule_missing".to_string());
        }
        if self.quality_rules.is_empty() {
            warnings.push("quality_rule_missing".to_string());
        }
        MatrixSourcePackValidation {
            source_pack_id: self.source_pack_id.clone(),
            status: if blockers.is_empty() {
                "ready".to_string()
            } else {
                "blocked".to_string()
            },
            blockers,
            warnings,
            validated_at: Utc::now(),
        }
    }
}
