use chrono::Utc;
use matrix_core::{
    MatrixComputeJobInput, MatrixDataPlane, MatrixDataPlaneCapability, MatrixDataPlaneHealth,
    MatrixDataPlaneIngestPlan, MatrixDataPlaneIngestPlanInput, MatrixDataPlaneWatermark,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatrixSqliteDataPlane {
    pub watermark_count: u64,
}

impl MatrixSqliteDataPlane {
    #[must_use]
    pub fn new(watermark_count: u64) -> Self {
        Self { watermark_count }
    }
}

impl MatrixDataPlane for MatrixSqliteDataPlane {
    fn health(&self) -> MatrixDataPlaneHealth {
        MatrixDataPlaneHealth {
            provider: "sqlite_control_store".to_string(),
            mode: "control_plane_embedded_data_plane".to_string(),
            status: "pilot_ready".to_string(),
            capabilities: vec![
                capability(
                    "fact_batch_plan",
                    "Plans governed fact batches before ingest.",
                ),
                capability("idempotency_key", "Derives stable batch idempotency keys."),
                capability(
                    "watermark_contract",
                    "Tracks source partition high watermarks.",
                ),
                capability(
                    "replay_policy",
                    "Declares replay behavior for batch recovery.",
                ),
            ],
            watermark_count: self.watermark_count,
            checked_at: Utc::now(),
        }
    }

    fn plan_ingest(&self, input: MatrixDataPlaneIngestPlanInput) -> MatrixDataPlaneIngestPlan {
        let partition_ref = input
            .partition_ref
            .unwrap_or_else(|| "default-partition".to_string());
        let high_watermark = input
            .high_watermark
            .unwrap_or_else(|| Utc::now().to_rfc3339());
        let idempotency_key = stable_key(&[
            input.source_ref.as_str(),
            input.fact_type.as_str(),
            partition_ref.as_str(),
            high_watermark.as_str(),
            input.raw_checksum.as_deref().unwrap_or("no-checksum"),
        ]);
        let batch_id = format!("data-plane-batch-{idempotency_key}");
        let compute_jobs = input
            .metric_ids
            .iter()
            .map(|metric_id| MatrixComputeJobInput {
                job_id: Some(format!("compute-job-{batch_id}-{metric_id}")),
                trigger_fact_type: input.fact_type.clone(),
                trigger_fact_refs: vec![format!("matrix:data-plane-batch:{batch_id}")],
                entity_scope: None,
                period: Some(partition_ref.clone()),
                metric_ids: vec![metric_id.clone()],
                priority: Some(0.72),
            })
            .collect::<Vec<_>>();
        let now = Utc::now();
        MatrixDataPlaneIngestPlan {
            batch_id: batch_id.clone(),
            source_ref: input.source_ref.clone(),
            fact_type: input.fact_type.clone(),
            partition_ref: partition_ref.clone(),
            idempotency_key,
            replay_policy: "replace_partition_by_idempotency_key".to_string(),
            estimated_rows: input.estimated_rows.unwrap_or(0),
            affected_metric_ids: input.metric_ids,
            compute_jobs,
            watermark: MatrixDataPlaneWatermark {
                source_ref: input.source_ref,
                fact_type: input.fact_type,
                partition_ref,
                high_watermark,
                last_batch_id: batch_id,
                updated_at: now,
            },
            planned_at: now,
        }
    }
}

fn capability(capability_id: &str, description: &str) -> MatrixDataPlaneCapability {
    MatrixDataPlaneCapability {
        capability_id: capability_id.to_string(),
        status: "available".to_string(),
        description: description.to_string(),
    }
}

fn stable_key(parts: &[&str]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update(b"\0");
    }
    let digest = hasher.finalize();
    format!("{digest:x}").chars().take(16).collect()
}
