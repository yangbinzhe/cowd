use serde_json::Value;

use crate::matrix::{
    MatrixDataPlaneIngestPlan, MatrixDataPlaneIngestPlanInput, MatrixDataPlaneWatermark,
    MatrixEvidencePacket, MatrixFact, MatrixSourceDeltaPlan, MatrixSourceEntityMapping,
    MatrixSourceFactMapping, MatrixSourcePack,
};

use super::{
    CowdDeltaPlan, CowdIngestPlan, CowdStructuredComputeRequest, CowdStructuredEvidence,
    CowdStructuredEvidenceSourceRef, CowdStructuredFact, CowdStructuredIngestPlanInput,
    CowdStructuredMapping, CowdStructuredSource, CowdStructuredTargetKind, CowdWatermark,
};

impl From<&MatrixSourcePack> for CowdStructuredSource {
    fn from(pack: &MatrixSourcePack) -> Self {
        let mut mappings = pack
            .entity_mappings
            .iter()
            .map(|mapping| CowdStructuredMapping::from_matrix_entity(&pack.source_pack_id, mapping))
            .collect::<Vec<_>>();
        mappings.extend(
            pack.fact_mappings.iter().map(|mapping| {
                CowdStructuredMapping::from_matrix_fact(&pack.source_pack_id, mapping)
            }),
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

impl CowdStructuredMapping {
    #[must_use]
    pub fn from_matrix_entity(source_ref: &str, mapping: &MatrixSourceEntityMapping) -> Self {
        Self {
            mapping_id: format!("{source_ref}:entity:{}", mapping.source_entity),
            source_ref: source_ref.to_string(),
            source_collection: mapping.source_entity.clone(),
            target_kind: CowdStructuredTargetKind::Entity,
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

impl From<&MatrixSourceEntityMapping> for CowdStructuredMapping {
    fn from(mapping: &MatrixSourceEntityMapping) -> Self {
        Self::from_matrix_entity("matrix:inline-source", mapping)
    }
}

impl From<&MatrixSourceFactMapping> for CowdStructuredMapping {
    fn from(mapping: &MatrixSourceFactMapping) -> Self {
        Self::from_matrix_fact("matrix:inline-source", mapping)
    }
}

impl From<&MatrixFact> for CowdStructuredFact {
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

impl From<&MatrixEvidencePacket> for CowdStructuredEvidence {
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

impl From<&MatrixDataPlaneWatermark> for CowdWatermark {
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

impl From<&MatrixDataPlaneIngestPlan> for CowdIngestPlan {
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

impl From<&MatrixSourceDeltaPlan> for CowdDeltaPlan {
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

impl From<CowdStructuredIngestPlanInput> for MatrixDataPlaneIngestPlanInput {
    fn from(input: CowdStructuredIngestPlanInput) -> Self {
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
    use chrono::{DateTime, Utc};

    use super::*;
    use crate::matrix::{
        MatrixComputeJobInput, MatrixDataPlaneIngestPlan, MatrixDataPlaneWatermark,
        MatrixEvidenceSourceRef, MatrixFactInput, MatrixSourceEntityMapping,
        MatrixSourceFactMapping, MatrixSourcePack,
    };

    #[test]
    fn structured_source_from_matrix_pack_preserves_owner_policy_and_mappings() {
        let pack = MatrixSourcePack {
            source_pack_id: "pack-1".to_string(),
            source_name: "erp".to_string(),
            owner: "ops".to_string(),
            access_mode: "read_only".to_string(),
            refresh_mode: "incremental".to_string(),
            entity_mappings: vec![MatrixSourceEntityMapping {
                source_entity: "item_master".to_string(),
                matrix_entity_type: "material".to_string(),
                source_key_field: "item_id".to_string(),
            }],
            fact_mappings: vec![MatrixSourceFactMapping {
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
        assert_eq!(source.domain.as_deref(), Some("matrix"));
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
    fn ingest_plan_from_matrix_preserves_idempotency_watermark_and_jobs() {
        let plan = MatrixDataPlaneIngestPlan {
            batch_id: "batch-1".to_string(),
            source_ref: "pack-1".to_string(),
            fact_type: "inventory_balance".to_string(),
            partition_ref: "2026-06-14".to_string(),
            idempotency_key: "idem-1".to_string(),
            replay_policy: "replace_partition_by_idempotency_key".to_string(),
            estimated_rows: 42,
            affected_metric_ids: vec!["stock_on_hand".to_string()],
            compute_jobs: vec![MatrixComputeJobInput {
                job_id: Some("job-1".to_string()),
                trigger_fact_type: "inventory_balance".to_string(),
                trigger_fact_refs: vec!["matrix:data-plane-batch:batch-1".to_string()],
                entity_scope: Some("site:cn-1".to_string()),
                period: Some("2026-06-14".to_string()),
                metric_ids: vec!["stock_on_hand".to_string()],
                priority: Some(0.9),
            }],
            watermark: MatrixDataPlaneWatermark {
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
    fn structured_fact_and_evidence_from_matrix_preserve_refs_and_confidence() {
        let fact = MatrixFact::from_input(MatrixFactInput {
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
        let mut packet = MatrixEvidencePacket::new("inventory changed");
        packet.packet_id = "packet-1".to_string();
        packet.source_refs.push(MatrixEvidenceSourceRef {
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
}
