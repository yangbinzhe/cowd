use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatrixSourceKind {
    Api,
    Db,
    File,
    Rpa,
    Manual,
    Connector,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixSourceSnapshot {
    pub snapshot_id: String,
    #[serde(default)]
    pub source_pack_id: Option<String>,
    pub source_system: String,
    pub source_kind: MatrixSourceKind,
    #[serde(default)]
    pub resource_ref: Option<String>,
    #[serde(default)]
    pub business_period: Option<String>,
    pub captured_at: DateTime<Utc>,
    pub schema_version: String,
    pub row_count: u64,
    #[serde(default)]
    pub checksum: Option<String>,
    pub confidence: f32,
    #[serde(default)]
    pub metadata: Value,
}

impl MatrixSourceSnapshot {
    #[must_use]
    pub fn new(
        source_system: impl Into<String>,
        source_kind: MatrixSourceKind,
        schema_version: impl Into<String>,
    ) -> Self {
        Self {
            snapshot_id: format!("snapshot-{}", uuid::Uuid::new_v4()),
            source_pack_id: None,
            source_system: source_system.into(),
            source_kind,
            resource_ref: None,
            business_period: None,
            captured_at: Utc::now(),
            schema_version: schema_version.into(),
            row_count: 0,
            checksum: None,
            confidence: 1.0,
            metadata: Value::Null,
        }
    }

    #[must_use]
    pub fn from_input(input: MatrixSourceSnapshotInput) -> Self {
        let mut snapshot = Self::new(
            input.source_system,
            input.source_kind,
            input
                .schema_version
                .unwrap_or_else(|| "unknown".to_string()),
        );
        snapshot.snapshot_id = input
            .snapshot_id
            .unwrap_or_else(|| format!("snapshot-{}", Uuid::new_v4()));
        snapshot.source_pack_id = input.source_pack_id;
        snapshot.resource_ref = input.resource_ref;
        snapshot.business_period = input.business_period;
        snapshot.captured_at = input.captured_at.unwrap_or_else(Utc::now);
        snapshot.row_count = input.row_count.unwrap_or(0);
        snapshot.checksum = input.checksum;
        snapshot.confidence = input.confidence.unwrap_or(1.0).clamp(0.0, 1.0);
        snapshot.metadata = input.metadata;
        snapshot
    }

    #[must_use]
    pub fn reference(&self) -> String {
        format!("matrix:source_snapshot:{}", self.snapshot_id)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixSourceSnapshotInput {
    #[serde(default)]
    pub snapshot_id: Option<String>,
    #[serde(default)]
    pub source_pack_id: Option<String>,
    pub source_system: String,
    pub source_kind: MatrixSourceKind,
    #[serde(default)]
    pub resource_ref: Option<String>,
    #[serde(default)]
    pub business_period: Option<String>,
    #[serde(default)]
    pub captured_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub schema_version: Option<String>,
    #[serde(default)]
    pub row_count: Option<u64>,
    #[serde(default)]
    pub checksum: Option<String>,
    #[serde(default)]
    pub confidence: Option<f32>,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixSourceSnapshotPlan {
    pub source_pack_id: String,
    pub source_ref: String,
    pub source_kind: MatrixSourceKind,
    pub access_mode: String,
    pub refresh_mode: String,
    pub estimated_rows: u64,
    #[serde(default)]
    pub fact_types: Vec<String>,
    #[serde(default)]
    pub affected_metric_ids: Vec<String>,
    #[serde(default)]
    pub quality_warnings: Vec<String>,
    pub planned_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixSourceSnapshotApplyReport {
    pub snapshot_id: String,
    pub source_pack_id: String,
    pub status: String,
    pub row_count: u64,
    pub fact_count: usize,
    pub relation_count: usize,
    pub attention_count: usize,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub fact_refs: Vec<String>,
    pub applied_at: DateTime<Utc>,
}
