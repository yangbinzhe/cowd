use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::MatrixComputeJobInput;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixDataPlaneCapability {
    pub capability_id: String,
    pub status: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixDataPlaneHealth {
    pub provider: String,
    pub mode: String,
    pub status: String,
    #[serde(default)]
    pub capabilities: Vec<MatrixDataPlaneCapability>,
    pub watermark_count: u64,
    pub checked_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixDataPlaneWatermark {
    pub source_ref: String,
    pub fact_type: String,
    pub partition_ref: String,
    pub high_watermark: String,
    pub last_batch_id: String,
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub adapter_id: Option<String>,
    #[serde(default)]
    pub strategy: Option<String>,
    #[serde(default)]
    pub table: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub offset: Option<u64>,
    #[serde(default)]
    pub checksum: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
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
    pub expected_revision: Option<u64>,
    #[serde(default)]
    pub adapter_id: Option<String>,
    #[serde(default)]
    pub strategy: Option<String>,
    #[serde(default)]
    pub table: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub offset: Option<u64>,
    #[serde(default)]
    pub metric_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixDataPlaneIngestPlan {
    pub batch_id: String,
    pub source_ref: String,
    pub fact_type: String,
    pub partition_ref: String,
    pub idempotency_key: String,
    pub replay_policy: String,
    #[serde(default)]
    pub expected_revision: Option<u64>,
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
