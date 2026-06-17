use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
pub struct CowdStructuredIngestPlanInput {
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
}
