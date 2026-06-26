use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::MatrixComputeJobInput;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixDataPlaneCapability {
    pub capability_id: String,
    pub status: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixDataPlaneHealth {
    pub provider: String,
    pub mode: String,
    pub status: String,
    #[serde(default)]
    pub capabilities: Vec<MatrixDataPlaneCapability>,
    pub watermark_count: u64,
    pub checked_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixDataPlaneWatermark {
    pub source_ref: String,
    pub fact_type: String,
    pub partition_ref: String,
    pub high_watermark: String,
    pub last_batch_id: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixDataPlaneIngestPlanInput {
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixDataPlaneIngestPlan {
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
    pub compute_jobs: Vec<MatrixComputeJobInput>,
    pub watermark: MatrixDataPlaneWatermark,
    pub planned_at: DateTime<Utc>,
}

pub trait MatrixDataPlane {
    fn health(&self) -> MatrixDataPlaneHealth;
    fn plan_ingest(&self, input: MatrixDataPlaneIngestPlanInput) -> MatrixDataPlaneIngestPlan;
}
