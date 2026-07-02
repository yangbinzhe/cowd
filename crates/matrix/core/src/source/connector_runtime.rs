use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{MatrixSourceDeltaPlan, MatrixSourcePack};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixConnectorRunInput {
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub resource_ref: Option<String>,
    #[serde(default)]
    pub partition_ref: Option<String>,
    #[serde(default)]
    pub credential_ref: Option<String>,
    #[serde(default)]
    pub expected_rows: Option<u64>,
    #[serde(default)]
    pub checksum: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixConnectorQualityReport {
    pub status: String,
    #[serde(default)]
    pub blockers: Vec<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
    pub score: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixConnectorReceipt {
    pub receipt_id: String,
    pub status: String,
    pub message: String,
    pub retryable: bool,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixConnectorRun {
    pub run_id: String,
    pub source_pack_id: String,
    pub connector_kind: String,
    pub mode: String,
    #[serde(default)]
    pub resource_ref: Option<String>,
    #[serde(default)]
    pub partition_ref: Option<String>,
    #[serde(default)]
    pub credential_ref: Option<String>,
    pub status: String,
    pub expected_rows: u64,
    pub mapped_fact_types: Vec<String>,
    pub affected_metric_ids: Vec<String>,
    pub quality_report: MatrixConnectorQualityReport,
    pub receipt: MatrixConnectorReceipt,
    #[serde(default)]
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl MatrixConnectorRun {
    #[must_use]
    pub fn from_source_pack(
        source_pack: &MatrixSourcePack,
        delta_plan: &MatrixSourceDeltaPlan,
        input: MatrixConnectorRunInput,
    ) -> Self {
        let now = Utc::now();
        let mode = input.mode.clone().unwrap_or_else(|| "plan".to_string());
        let connector_kind = connector_kind(source_pack);
        let quality_report = quality_report(source_pack, &input);
        let status = if quality_report.status == "blocked" {
            "blocked"
        } else {
            "planned"
        };
        let retryable = status != "planned";
        Self {
            run_id: input
                .run_id
                .unwrap_or_else(|| format!("connector-run-{}", uuid::Uuid::new_v4())),
            source_pack_id: source_pack.source_pack_id.clone(),
            connector_kind,
            mode,
            resource_ref: input.resource_ref,
            partition_ref: input.partition_ref,
            credential_ref: input.credential_ref,
            status: status.to_string(),
            expected_rows: input.expected_rows.unwrap_or(0),
            mapped_fact_types: delta_plan.fact_types.clone(),
            affected_metric_ids: delta_plan.affected_metric_ids.clone(),
            receipt: MatrixConnectorReceipt {
                receipt_id: format!("connector-receipt-{}", uuid::Uuid::new_v4()),
                status: status.to_string(),
                message: receipt_message(status),
                retryable,
                recorded_at: now,
            },
            quality_report,
            metadata: serde_json::json!({
                "refresh_mode": source_pack.refresh_mode,
                "access_mode": source_pack.access_mode,
                "checksum": input.checksum,
            }),
            created_at: now,
            updated_at: now,
        }
    }
}

fn connector_kind(source_pack: &MatrixSourcePack) -> String {
    match source_pack.access_mode.as_str() {
        "batch_file" | "file" => "batch_file_connector",
        "db_view" | "database_view" => "database_view_connector",
        "manual_upload" | "manual" => "manual_upload_connector",
        "api" => "api_connector",
        other => other,
    }
    .to_string()
}

fn quality_report(
    source_pack: &MatrixSourcePack,
    input: &MatrixConnectorRunInput,
) -> MatrixConnectorQualityReport {
    let validation = source_pack.validate();
    let mut blockers = validation.blockers;
    let mut warnings = validation.warnings;
    if input
        .resource_ref
        .as_deref()
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        blockers.push("resource_ref_required".to_string());
    }
    if input.credential_ref.is_none()
        && matches!(
            source_pack.access_mode.as_str(),
            "api" | "db_view" | "database_view"
        )
    {
        warnings.push("credential_ref_missing".to_string());
    }
    let status = if blockers.is_empty() {
        "ready"
    } else {
        "blocked"
    };
    let score = if blockers.is_empty() {
        (1.0 - warnings.len() as f32 * 0.08).max(0.7)
    } else {
        0.25
    };
    MatrixConnectorQualityReport {
        status: status.to_string(),
        blockers,
        warnings,
        score,
    }
}

fn receipt_message(status: &str) -> String {
    match status {
        "blocked" => "connector run blocked by source pack quality gate".to_string(),
        _ => {
            "connector run planned; execute through source snapshot capture/apply for a real receipt"
                .to_string()
        }
    }
}
