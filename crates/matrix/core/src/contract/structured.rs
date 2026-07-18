use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    MatrixDataPlaneIngestPlan, MatrixDataPlaneIngestPlanInput, MatrixDataPlaneWatermark,
    MatrixEvidencePacket, MatrixFact, MatrixSourceDeltaPlan, MatrixSourceEntityMapping,
    MatrixSourceFactMapping, MatrixSourcePack,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StructuredTargetKind {
    Entity,
    Fact,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StructuredMapping {
    pub mapping_id: String,
    pub source_ref: String,
    pub source_collection: String,
    pub target_kind: StructuredTargetKind,
    pub target_type: String,
    #[serde(default)]
    pub metric_key: Option<String>,
    #[serde(default)]
    pub key_fields: Vec<String>,
    #[serde(default)]
    pub measure_fields: Vec<String>,
    #[serde(default)]
    pub dedup_key: Option<String>,
    #[serde(default)]
    pub delta_signature: Option<String>,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StructuredSource {
    pub source_id: String,
    pub source_name: String,
    #[serde(default)]
    pub domain: Option<String>,
    pub owner: String,
    pub access_mode: String,
    pub refresh_mode: String,
    #[serde(default)]
    pub mappings: Vec<StructuredMapping>,
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
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StructuredFact {
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
    #[serde(default)]
    pub domain: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StructuredEvidenceSourceRef {
    pub kind: String,
    pub reference: String,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StructuredEvidence {
    pub evidence_id: String,
    #[serde(default)]
    pub attention_id: Option<String>,
    pub problem_statement: String,
    #[serde(default)]
    pub domain: Option<String>,
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
    pub source_refs: Vec<StructuredEvidenceSourceRef>,
    #[serde(default)]
    pub missing_evidence: Vec<String>,
    pub confidence: f32,
    pub token_budget: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StructuredWatermark {
    pub source_ref: String,
    pub fact_type: String,
    pub partition_ref: String,
    pub high_watermark: String,
    pub last_batch_id: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StructuredComputeRequest {
    #[serde(default)]
    pub job_id: Option<String>,
    pub trigger_fact_type: String,
    #[serde(default)]
    pub trigger_fact_refs: Vec<String>,
    #[serde(default)]
    pub entity_scope: Option<String>,
    #[serde(default)]
    pub period: Option<String>,
    #[serde(default)]
    pub metric_ids: Vec<String>,
    #[serde(default)]
    pub priority: Option<f32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StructuredIngestPlanInput {
    pub source_ref: String,
    pub fact_type: String,
    #[serde(default)]
    pub partition_ref: Option<String>,
    #[serde(default)]
    pub high_watermark: Option<String>,
    #[serde(default)]
    pub estimated_rows: Option<u64>,
    #[serde(default)]
    pub raw_checksum: Option<String>,
    #[serde(default)]
    pub metric_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StructuredIngestPlan {
    pub batch_id: String,
    pub source_ref: String,
    pub fact_type: String,
    pub partition_ref: String,
    pub idempotency_key: String,
    pub replay_policy: String,
    pub estimated_rows: u64,
    #[serde(default)]
    pub affected_metric_ids: Vec<String>,
    #[serde(default)]
    pub compute_requests: Vec<StructuredComputeRequest>,
    pub watermark: StructuredWatermark,
    pub planned_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StructuredDeltaPlan {
    pub source_ref: String,
    #[serde(default)]
    pub fact_types: Vec<String>,
    #[serde(default)]
    pub affected_metric_ids: Vec<String>,
    pub compute_scope: String,
    pub planned_at: DateTime<Utc>,
}

impl From<&MatrixSourcePack> for StructuredSource {
    fn from(pack: &MatrixSourcePack) -> Self {
        let mut mappings = pack
            .entity_mappings
            .iter()
            .map(|mapping| StructuredMapping::from_matrix_entity(&pack.source_pack_id, mapping))
            .collect::<Vec<_>>();
        mappings.extend(
            pack.fact_mappings
                .iter()
                .map(|mapping| StructuredMapping::from_matrix_fact(&pack.source_pack_id, mapping)),
        );

        Self {
            source_id: pack.source_pack_id.clone(),
            source_name: pack.source_name.clone(),
            domain: Some("matrix".to_string()),
            owner: pack.owner.clone(),
            access_mode: pack.access_mode.clone(),
            refresh_mode: pack.refresh_mode.clone(),
            mappings,
            reconciliation_rules: pack.reconciliation_rules.clone(),
            quality_rules: pack.quality_rules.clone(),
            freshness_sla: pack.freshness_sla.clone(),
            security_policy: pack.security_policy.clone(),
            metadata: pack.metadata.clone(),
            created_at: pack.created_at,
            updated_at: pack.updated_at,
        }
    }
}

impl StructuredMapping {
    #[must_use]
    pub fn from_matrix_entity(source_ref: &str, mapping: &MatrixSourceEntityMapping) -> Self {
        Self {
            mapping_id: format!("{source_ref}:entity:{}", mapping.source_entity),
            source_ref: source_ref.to_string(),
            source_collection: mapping.source_entity.clone(),
            target_kind: StructuredTargetKind::Entity,
            target_type: mapping.matrix_entity_type.clone(),
            metric_key: None,
            key_fields: vec![mapping.source_key_field.clone()],
            measure_fields: Vec::new(),
            dedup_key: None,
            delta_signature: None,
            metadata: Value::Null,
        }
    }

    #[must_use]
    pub fn from_matrix_fact(source_ref: &str, mapping: &MatrixSourceFactMapping) -> Self {
        Self {
            mapping_id: format!("{source_ref}:fact:{}", mapping.fact_type),
            source_ref: source_ref.to_string(),
            source_collection: mapping.source_table.clone(),
            target_kind: StructuredTargetKind::Fact,
            target_type: mapping.fact_type.clone(),
            metric_key: Some(mapping.metric_key.clone()),
            key_fields: mapping.entity_ref_fields.clone(),
            measure_fields: mapping.measure_fields.clone(),
            dedup_key: Some(mapping.dedup_key.clone()),
            delta_signature: Some(mapping.delta_signature.clone()),
            metadata: Value::Null,
        }
    }
}

impl From<&MatrixFact> for StructuredFact {
    fn from(fact: &MatrixFact) -> Self {
        Self {
            fact_id: fact.fact_id.clone(),
            snapshot_id: fact.snapshot_id.clone(),
            fact_type: fact.fact_type.clone(),
            entity_refs: fact.entity_refs.clone(),
            metric_key: fact.metric_key.clone(),
            dimensions: fact.dimensions.clone(),
            measures: fact.measures.clone(),
            event_time: fact.event_time,
            valid_from: fact.valid_from,
            valid_to: fact.valid_to,
            source_ref: fact.source_ref.clone(),
            confidence: fact.confidence,
            raw_hash: fact.raw_hash.clone(),
            domain: Some("matrix".to_string()),
        }
    }
}

impl From<&MatrixEvidencePacket> for StructuredEvidence {
    fn from(packet: &MatrixEvidencePacket) -> Self {
        Self {
            evidence_id: packet.packet_id.clone(),
            attention_id: packet.attention_id.clone(),
            problem_statement: packet.problem_statement.clone(),
            domain: Some("matrix".to_string()),
            business_context: packet.business_context.clone(),
            metric_evidence: packet.metric_evidence.clone(),
            change_evidence: packet.change_evidence.clone(),
            anomaly_evidence: packet.anomaly_evidence.clone(),
            attribution_candidates: packet.attribution_candidates.clone(),
            impact_paths: packet.impact_paths.clone(),
            source_refs: packet
                .source_refs
                .iter()
                .map(|source| StructuredEvidenceSourceRef {
                    kind: source.kind.clone(),
                    reference: source.reference.clone(),
                    summary: source.summary.clone(),
                })
                .collect(),
            missing_evidence: packet.missing_evidence.clone(),
            confidence: packet.confidence,
            token_budget: packet.token_budget,
            created_at: packet.created_at,
        }
    }
}

impl From<&MatrixDataPlaneWatermark> for StructuredWatermark {
    fn from(watermark: &MatrixDataPlaneWatermark) -> Self {
        Self {
            source_ref: watermark.source_ref.clone(),
            fact_type: watermark.fact_type.clone(),
            partition_ref: watermark.partition_ref.clone(),
            high_watermark: watermark.high_watermark.clone(),
            last_batch_id: watermark.last_batch_id.clone(),
            updated_at: watermark.updated_at,
        }
    }
}

impl From<&MatrixDataPlaneIngestPlan> for StructuredIngestPlan {
    fn from(plan: &MatrixDataPlaneIngestPlan) -> Self {
        Self {
            batch_id: plan.batch_id.clone(),
            source_ref: plan.source_ref.clone(),
            fact_type: plan.fact_type.clone(),
            partition_ref: plan.partition_ref.clone(),
            idempotency_key: plan.idempotency_key.clone(),
            replay_policy: plan.replay_policy.clone(),
            estimated_rows: plan.estimated_rows,
            affected_metric_ids: plan.affected_metric_ids.clone(),
            compute_requests: plan
                .compute_jobs
                .iter()
                .map(|job| StructuredComputeRequest {
                    job_id: job.job_id.clone(),
                    trigger_fact_type: job.trigger_fact_type.clone(),
                    trigger_fact_refs: job.trigger_fact_refs.clone(),
                    entity_scope: job.entity_scope.clone(),
                    period: job.period.clone(),
                    metric_ids: job.metric_ids.clone(),
                    priority: job.priority,
                })
                .collect(),
            watermark: StructuredWatermark::from(&plan.watermark),
            planned_at: plan.planned_at,
        }
    }
}

impl From<&MatrixSourceDeltaPlan> for StructuredDeltaPlan {
    fn from(plan: &MatrixSourceDeltaPlan) -> Self {
        Self {
            source_ref: plan.source_pack_id.clone(),
            fact_types: plan.fact_types.clone(),
            affected_metric_ids: plan.affected_metric_ids.clone(),
            compute_scope: plan.compute_scope.clone(),
            planned_at: plan.planned_at,
        }
    }
}

impl From<StructuredIngestPlanInput> for MatrixDataPlaneIngestPlanInput {
    fn from(input: StructuredIngestPlanInput) -> Self {
        Self {
            source_ref: input.source_ref,
            fact_type: input.fact_type,
            partition_ref: input.partition_ref,
            high_watermark: input.high_watermark,
            estimated_rows: input.estimated_rows,
            raw_checksum: input.raw_checksum,
            metric_ids: input.metric_ids,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_contract_stays_matrix_neutral() {
        let mapping = StructuredMapping {
            mapping_id: "map-1".to_string(),
            source_ref: "source-1".to_string(),
            source_collection: "orders".to_string(),
            target_kind: StructuredTargetKind::Fact,
            target_type: "order".to_string(),
            metric_key: None,
            key_fields: vec!["order_id".to_string()],
            measure_fields: vec!["amount".to_string()],
            dedup_key: None,
            delta_signature: None,
            metadata: Value::Null,
        };

        assert_eq!(mapping.target_kind, StructuredTargetKind::Fact);
    }
}
