use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::iacc::{
    IaccDataPlaneIngestPlan, IaccDataPlaneWatermark, IaccEvidencePacket, IaccFact,
    IaccSourceDeltaPlan, IaccSourceEntityMapping, IaccSourceFactMapping, IaccSourcePack,
};
use crate::{ContextAuthority, ContextItem, ContextRole, ContextSourceKind, ContextVisibility};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CowdStructuredTargetKind {
    Entity,
    Fact,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CowdStructuredSource {
    pub source_id: String,
    pub source_name: String,
    pub domain: Option<String>,
    pub owner: String,
    pub access_mode: String,
    pub refresh_mode: String,
    #[serde(default)]
    pub mappings: Vec<CowdStructuredMapping>,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CowdStructuredMapping {
    pub mapping_id: String,
    pub source_ref: String,
    pub source_collection: String,
    pub target_kind: CowdStructuredTargetKind,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CowdStructuredFact {
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CowdStructuredEvidenceSourceRef {
    pub kind: String,
    pub reference: String,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CowdStructuredEvidence {
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
    pub source_refs: Vec<CowdStructuredEvidenceSourceRef>,
    #[serde(default)]
    pub missing_evidence: Vec<String>,
    pub confidence: f32,
    pub token_budget: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CowdWatermark {
    pub source_ref: String,
    pub fact_type: String,
    pub partition_ref: String,
    pub high_watermark: String,
    pub last_batch_id: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CowdStructuredComputeRequest {
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CowdIngestPlan {
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
    pub compute_requests: Vec<CowdStructuredComputeRequest>,
    pub watermark: CowdWatermark,
    pub planned_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CowdDeltaPlan {
    pub source_ref: String,
    #[serde(default)]
    pub fact_types: Vec<String>,
    #[serde(default)]
    pub affected_metric_ids: Vec<String>,
    pub compute_scope: String,
    pub planned_at: DateTime<Utc>,
}

impl From<&IaccSourcePack> for CowdStructuredSource {
    fn from(pack: &IaccSourcePack) -> Self {
        let mut mappings = pack
            .entity_mappings
            .iter()
            .map(|mapping| CowdStructuredMapping::from_iacc_entity(&pack.source_pack_id, mapping))
            .collect::<Vec<_>>();
        mappings.extend(
            pack.fact_mappings.iter().map(|mapping| {
                CowdStructuredMapping::from_iacc_fact(&pack.source_pack_id, mapping)
            }),
        );

        Self {
            source_id: pack.source_pack_id.clone(),
            source_name: pack.source_name.clone(),
            domain: Some("iacc".to_string()),
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

impl CowdStructuredMapping {
    #[must_use]
    pub fn from_iacc_entity(source_ref: &str, mapping: &IaccSourceEntityMapping) -> Self {
        Self {
            mapping_id: format!("{source_ref}:entity:{}", mapping.source_entity),
            source_ref: source_ref.to_string(),
            source_collection: mapping.source_entity.clone(),
            target_kind: CowdStructuredTargetKind::Entity,
            target_type: mapping.iacc_entity_type.clone(),
            metric_key: None,
            key_fields: vec![mapping.source_key_field.clone()],
            measure_fields: Vec::new(),
            dedup_key: None,
            delta_signature: None,
            metadata: Value::Null,
        }
    }

    #[must_use]
    pub fn from_iacc_fact(source_ref: &str, mapping: &IaccSourceFactMapping) -> Self {
        Self {
            mapping_id: format!("{source_ref}:fact:{}", mapping.fact_type),
            source_ref: source_ref.to_string(),
            source_collection: mapping.source_table.clone(),
            target_kind: CowdStructuredTargetKind::Fact,
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

impl From<&IaccSourceEntityMapping> for CowdStructuredMapping {
    fn from(mapping: &IaccSourceEntityMapping) -> Self {
        Self::from_iacc_entity("iacc:inline-source", mapping)
    }
}

impl From<&IaccSourceFactMapping> for CowdStructuredMapping {
    fn from(mapping: &IaccSourceFactMapping) -> Self {
        Self::from_iacc_fact("iacc:inline-source", mapping)
    }
}

impl From<&IaccFact> for CowdStructuredFact {
    fn from(fact: &IaccFact) -> Self {
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
            domain: Some("iacc".to_string()),
        }
    }
}

impl CowdStructuredFact {
    #[must_use]
    pub fn stable_ref(&self) -> String {
        format!("structured-fact:{}", self.fact_id)
    }

    #[must_use]
    pub fn memory_summary(&self) -> CowdStructuredMemorySummary {
        CowdStructuredMemorySummary {
            reference: self.stable_ref(),
            title: format!("{} fact {}", self.fact_type, self.fact_id),
            summary: format!(
                "Structured fact {} of type {} references {} entities and metric {}.",
                self.fact_id,
                self.fact_type,
                self.entity_refs.len(),
                self.metric_key.as_deref().unwrap_or("none")
            ),
            source_ref: self.source_ref.clone(),
            confidence: self.confidence,
            raw_hash: self.raw_hash.clone(),
        }
    }
}

impl From<&IaccEvidencePacket> for CowdStructuredEvidence {
    fn from(packet: &IaccEvidencePacket) -> Self {
        Self {
            evidence_id: packet.packet_id.clone(),
            attention_id: packet.attention_id.clone(),
            problem_statement: packet.problem_statement.clone(),
            domain: Some("iacc".to_string()),
            business_context: packet.business_context.clone(),
            metric_evidence: packet.metric_evidence.clone(),
            change_evidence: packet.change_evidence.clone(),
            anomaly_evidence: packet.anomaly_evidence.clone(),
            attribution_candidates: packet.attribution_candidates.clone(),
            impact_paths: packet.impact_paths.clone(),
            source_refs: packet
                .source_refs
                .iter()
                .map(|source| CowdStructuredEvidenceSourceRef {
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

impl CowdStructuredEvidence {
    #[must_use]
    pub fn stable_ref(&self) -> String {
        format!("structured-evidence:{}", self.evidence_id)
    }

    #[must_use]
    pub fn memory_summary(&self) -> CowdStructuredMemorySummary {
        CowdStructuredMemorySummary {
            reference: self.stable_ref(),
            title: format!("Evidence {}", self.evidence_id),
            summary: format!(
                "{}. metric_evidence={}, change_evidence={}, source_refs={}, confidence={:.2}",
                self.problem_statement,
                self.metric_evidence.len(),
                self.change_evidence.len(),
                self.source_refs.len(),
                self.confidence
            ),
            source_ref: None,
            confidence: self.confidence,
            raw_hash: self.evidence_id.clone(),
        }
    }

    #[must_use]
    pub fn to_context_item(&self) -> ContextItem {
        let summary = self.memory_summary();
        let mut item = ContextItem::new(
            summary.reference,
            ContextSourceKind::Task,
            ContextRole::Evidence,
            summary.summary,
        );
        item.authority = ContextAuthority::Derived;
        item.visibility = ContextVisibility::Shared;
        item.score = self.confidence;
        item.evidence = self
            .source_refs
            .iter()
            .map(|source| source.reference.clone())
            .collect();
        item
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CowdStructuredMemorySummary {
    pub reference: String,
    pub title: String,
    pub summary: String,
    #[serde(default)]
    pub source_ref: Option<String>,
    pub confidence: f32,
    pub raw_hash: String,
}

impl From<&IaccDataPlaneWatermark> for CowdWatermark {
    fn from(watermark: &IaccDataPlaneWatermark) -> Self {
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

impl From<&IaccDataPlaneIngestPlan> for CowdIngestPlan {
    fn from(plan: &IaccDataPlaneIngestPlan) -> Self {
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
                .map(|job| CowdStructuredComputeRequest {
                    job_id: job.job_id.clone(),
                    trigger_fact_type: job.trigger_fact_type.clone(),
                    trigger_fact_refs: job.trigger_fact_refs.clone(),
                    entity_scope: job.entity_scope.clone(),
                    period: job.period.clone(),
                    metric_ids: job.metric_ids.clone(),
                    priority: job.priority,
                })
                .collect(),
            watermark: CowdWatermark::from(&plan.watermark),
            planned_at: plan.planned_at,
        }
    }
}

impl From<&IaccSourceDeltaPlan> for CowdDeltaPlan {
    fn from(plan: &IaccSourceDeltaPlan) -> Self {
        Self {
            source_ref: plan.source_pack_id.clone(),
            fact_types: plan.fact_types.clone(),
            affected_metric_ids: plan.affected_metric_ids.clone(),
            compute_scope: plan.compute_scope.clone(),
            planned_at: plan.planned_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iacc::{
        IaccComputeJobInput, IaccDataPlaneIngestPlan, IaccDataPlaneWatermark,
        IaccEvidenceSourceRef, IaccFactInput,
    };

    #[test]
    fn structured_source_from_iacc_pack_preserves_owner_policy_and_mappings() {
        let pack = IaccSourcePack {
            source_pack_id: "pack-1".to_string(),
            source_name: "erp".to_string(),
            owner: "ops".to_string(),
            access_mode: "read_only".to_string(),
            refresh_mode: "incremental".to_string(),
            entity_mappings: vec![IaccSourceEntityMapping {
                source_entity: "item_master".to_string(),
                iacc_entity_type: "material".to_string(),
                source_key_field: "item_id".to_string(),
            }],
            fact_mappings: vec![IaccSourceFactMapping {
                source_table: "inventory_snapshots".to_string(),
                fact_type: "inventory_balance".to_string(),
                metric_key: "stock_on_hand".to_string(),
                entity_ref_fields: vec!["item_id".to_string(), "site_id".to_string()],
                measure_fields: vec!["quantity".to_string()],
                dedup_key: "snapshot_id".to_string(),
                delta_signature: "updated_at".to_string(),
            }],
            reconciliation_rules: vec!["sum_by_site".to_string()],
            quality_rules: vec!["quantity_non_negative".to_string()],
            freshness_sla: Some("PT1H".to_string()),
            security_policy: Some("internal".to_string()),
            metadata: serde_json::json!({"system": "sap"}),
            created_at: DateTime::<Utc>::UNIX_EPOCH,
            updated_at: DateTime::<Utc>::UNIX_EPOCH,
        };

        let source = CowdStructuredSource::from(&pack);

        assert_eq!(source.source_id, "pack-1");
        assert_eq!(source.domain.as_deref(), Some("iacc"));
        assert_eq!(source.owner, "ops");
        assert_eq!(source.quality_rules, vec!["quantity_non_negative"]);
        assert_eq!(source.mappings.len(), 2);
        assert_eq!(
            source.mappings[0].target_kind,
            CowdStructuredTargetKind::Entity
        );
        assert_eq!(source.mappings[0].key_fields, vec!["item_id"]);
        assert_eq!(
            source.mappings[1].target_kind,
            CowdStructuredTargetKind::Fact
        );
        assert_eq!(
            source.mappings[1].metric_key.as_deref(),
            Some("stock_on_hand")
        );
        assert_eq!(source.mappings[1].measure_fields, vec!["quantity"]);
    }

    #[test]
    fn ingest_plan_from_iacc_preserves_idempotency_watermark_and_jobs() {
        let plan = IaccDataPlaneIngestPlan {
            batch_id: "batch-1".to_string(),
            source_ref: "pack-1".to_string(),
            fact_type: "inventory_balance".to_string(),
            partition_ref: "2026-06-14".to_string(),
            idempotency_key: "idem-1".to_string(),
            replay_policy: "replace_partition_by_idempotency_key".to_string(),
            estimated_rows: 42,
            affected_metric_ids: vec!["stock_on_hand".to_string()],
            compute_jobs: vec![IaccComputeJobInput {
                job_id: Some("job-1".to_string()),
                trigger_fact_type: "inventory_balance".to_string(),
                trigger_fact_refs: vec!["iacc:data-plane-batch:batch-1".to_string()],
                entity_scope: Some("site:cn-1".to_string()),
                period: Some("2026-06-14".to_string()),
                metric_ids: vec!["stock_on_hand".to_string()],
                priority: Some(0.9),
            }],
            watermark: IaccDataPlaneWatermark {
                source_ref: "pack-1".to_string(),
                fact_type: "inventory_balance".to_string(),
                partition_ref: "2026-06-14".to_string(),
                high_watermark: "2026-06-14T10:00:00Z".to_string(),
                last_batch_id: "batch-1".to_string(),
                updated_at: DateTime::<Utc>::UNIX_EPOCH,
            },
            planned_at: DateTime::<Utc>::UNIX_EPOCH,
        };

        let cowd_plan = CowdIngestPlan::from(&plan);

        assert_eq!(cowd_plan.idempotency_key, "idem-1");
        assert_eq!(cowd_plan.watermark.high_watermark, "2026-06-14T10:00:00Z");
        assert_eq!(cowd_plan.compute_requests.len(), 1);
        assert_eq!(
            cowd_plan.compute_requests[0].job_id.as_deref(),
            Some("job-1")
        );
        assert_eq!(
            cowd_plan.compute_requests[0].metric_ids,
            vec!["stock_on_hand"]
        );
    }

    #[test]
    fn structured_fact_and_evidence_from_iacc_preserve_refs_and_confidence() {
        let fact = IaccFact::from_input(IaccFactInput {
            fact_id: Some("fact-1".to_string()),
            snapshot_id: Some("snapshot-1".to_string()),
            fact_type: "inventory_balance".to_string(),
            entity_refs: vec!["material:123".to_string()],
            metric_key: Some("stock_on_hand".to_string()),
            dimensions: serde_json::json!({"site": "cn-1"}),
            measures: serde_json::json!({"quantity": 12}),
            event_time: Some(DateTime::<Utc>::UNIX_EPOCH),
            valid_from: None,
            valid_to: None,
            source_ref: Some("pack-1".to_string()),
            confidence: Some(0.8),
            raw_hash: Some("sha256:test".to_string()),
        });
        let mut packet = IaccEvidencePacket::new("inventory changed");
        packet.packet_id = "packet-1".to_string();
        packet.source_refs.push(IaccEvidenceSourceRef {
            kind: "fact".to_string(),
            reference: "fact-1".to_string(),
            summary: "inventory fact".to_string(),
        });
        packet
            .metric_evidence
            .push(serde_json::json!({"metric": "stock_on_hand"}));
        packet.confidence = 0.7;

        let structured_fact = CowdStructuredFact::from(&fact);
        let evidence = CowdStructuredEvidence::from(&packet);

        assert_eq!(structured_fact.fact_id, "fact-1");
        assert_eq!(structured_fact.entity_refs, vec!["material:123"]);
        assert_eq!(structured_fact.metric_key.as_deref(), Some("stock_on_hand"));
        assert_eq!(structured_fact.source_ref.as_deref(), Some("pack-1"));
        assert_eq!(structured_fact.raw_hash, "sha256:test");
        assert_eq!(evidence.evidence_id, "packet-1");
        assert_eq!(evidence.source_refs[0].reference, "fact-1");
        assert_eq!(evidence.metric_evidence.len(), 1);
        assert_eq!(evidence.confidence, 0.7);
    }

    #[test]
    fn structured_fact_memory_summary_keeps_reference_without_raw_payload_copy() {
        let fact = IaccFact::from_input(IaccFactInput {
            fact_id: Some("fact-1".to_string()),
            snapshot_id: Some("snapshot-1".to_string()),
            fact_type: "inventory_balance".to_string(),
            entity_refs: vec!["material:123".to_string(), "site:cn-1".to_string()],
            metric_key: Some("stock_on_hand".to_string()),
            dimensions: serde_json::json!({"large_dimension": "x".repeat(512)}),
            measures: serde_json::json!({"quantity": 12}),
            event_time: Some(DateTime::<Utc>::UNIX_EPOCH),
            valid_from: None,
            valid_to: None,
            source_ref: Some("pack-1".to_string()),
            confidence: Some(0.9),
            raw_hash: Some("sha256:fact".to_string()),
        });

        let summary = CowdStructuredFact::from(&fact).memory_summary();

        assert_eq!(summary.reference, "structured-fact:fact-1");
        assert_eq!(summary.source_ref.as_deref(), Some("pack-1"));
        assert_eq!(summary.raw_hash, "sha256:fact");
        assert!(!summary.summary.contains("large_dimension"));
        assert!(summary.summary.contains("references 2 entities"));
    }

    #[test]
    fn structured_evidence_context_item_uses_summary_and_source_refs() {
        let mut packet = IaccEvidencePacket::new("supplier delivery risk changed");
        packet.packet_id = "packet-1".to_string();
        packet.source_refs.push(IaccEvidenceSourceRef {
            kind: "fact".to_string(),
            reference: "fact-1".to_string(),
            summary: "inventory fact".to_string(),
        });
        packet
            .metric_evidence
            .push(serde_json::json!({"metric": "order_delivery_risk"}));
        packet.confidence = 0.75;

        let evidence = CowdStructuredEvidence::from(&packet);
        let item = evidence.to_context_item();

        assert_eq!(item.id, "structured-evidence:packet-1");
        assert_eq!(item.role, ContextRole::Evidence);
        assert_eq!(item.authority, ContextAuthority::Derived);
        assert_eq!(item.visibility, ContextVisibility::Shared);
        assert_eq!(item.score, 0.75);
        assert_eq!(item.evidence, vec!["fact-1"]);
        assert!(item.content.contains("metric_evidence=1"));
    }
}
