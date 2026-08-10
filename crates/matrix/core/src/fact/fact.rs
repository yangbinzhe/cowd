use chrono::{DateTime, Utc};
use fact_kernel::{
    hypothesis::HypothesisBoundary, matrix::MatrixFact as KernelMatrixFact, Confidence, FactId,
    FactKernelService, FactRecord, FactSource, FactStore, Provenance, SourceKind,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const AI_STRATEGY_DECISION_FACT: &str = "ai_strategy_decision";
pub const AI_VERIFICATION_RESULT_FACT: &str = "ai_verification_result";
pub const AI_TOOL_TRANSACTION_RESULT_FACT: &str = "ai_tool_transaction_result";
pub const AI_EXECUTION_GRAPH_QUALITY_FACT: &str = "ai_execution_graph_quality";
pub const AI_GROWTH_SIGNAL_FACT: &str = "ai_growth_signal";
pub const AI_EVAL_RESULT_FACT: &str = "ai_eval_result";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixFact {
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
}

impl MatrixFact {
    #[must_use]
    pub fn from_input(input: MatrixFactInput) -> Self {
        let event_time = input.event_time.unwrap_or_else(Utc::now);
        let fact_id = input
            .fact_id
            .unwrap_or_else(|| format!("fact-{}", uuid::Uuid::new_v4()));
        let snapshot_id = input
            .snapshot_id
            .unwrap_or_else(|| format!("snapshot-inline-{}", uuid::Uuid::new_v4()));
        let raw_hash = input.raw_hash.unwrap_or_else(|| {
            stable_hash(&serde_json::json!({
                "snapshot_id": snapshot_id,
                "fact_type": input.fact_type,
                "entity_refs": input.entity_refs,
                "metric_key": input.metric_key,
                "dimensions": input.dimensions,
                "measures": input.measures,
                "event_time": event_time,
                "source_ref": input.source_ref,
            }))
        });
        Self {
            fact_id,
            snapshot_id,
            fact_type: input.fact_type,
            entity_refs: input.entity_refs,
            metric_key: input.metric_key,
            dimensions: input.dimensions,
            measures: input.measures,
            event_time,
            valid_from: input.valid_from,
            valid_to: input.valid_to,
            source_ref: input.source_ref,
            confidence: input.confidence.unwrap_or(1.0),
            raw_hash,
        }
    }

    #[must_use]
    pub fn to_fact_kernel_matrix_fact(&self) -> KernelMatrixFact {
        KernelMatrixFact {
            id: FactId::from_string(self.fact_id.clone()),
            entity: self
                .entity_refs
                .first()
                .cloned()
                .unwrap_or_else(|| self.snapshot_id.clone()),
            predicate: self.fact_type.clone(),
            value: serde_json::json!({
                "metric_key": self.metric_key,
                "dimensions": self.dimensions,
                "measures": self.measures,
                "event_time": self.event_time,
                "valid_from": self.valid_from,
                "valid_to": self.valid_to,
                "raw_hash": self.raw_hash,
            }),
            source: FactSource {
                kind: SourceKind::Matrix,
                id: self
                    .source_ref
                    .clone()
                    .unwrap_or_else(|| self.snapshot_id.clone()),
                label: self.metric_key.clone(),
            },
            evidence: Vec::new(),
            confidence: Confidence::from_basis_points(confidence_basis_points(self.confidence)),
            boundary: HypothesisBoundary::observed(),
        }
    }

    #[must_use]
    pub fn from_fact_kernel_matrix_fact(
        fact: KernelMatrixFact,
        snapshot_id: impl Into<String>,
    ) -> Self {
        let dimensions = fact
            .value
            .get("dimensions")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        let measures = fact
            .value
            .get("measures")
            .cloned()
            .unwrap_or_else(|| fact.value.clone());
        let event_time = fact
            .value
            .get("event_time")
            .and_then(|value| serde_json::from_value(value.clone()).ok())
            .unwrap_or_else(Utc::now);
        Self::from_input(MatrixFactInput {
            fact_id: Some(fact.id.as_str().to_string()),
            snapshot_id: Some(snapshot_id.into()),
            fact_type: fact.predicate,
            entity_refs: vec![fact.entity],
            metric_key: fact.source.label,
            dimensions,
            measures,
            event_time: Some(event_time),
            valid_from: fact
                .value
                .get("valid_from")
                .and_then(|value| serde_json::from_value(value.clone()).ok()),
            valid_to: fact
                .value
                .get("valid_to")
                .and_then(|value| serde_json::from_value(value.clone()).ok()),
            source_ref: Some(fact.source.id),
            confidence: fact
                .confidence
                .basis_points()
                .map(|value| f32::from(value) / 10_000.0),
            raw_hash: fact
                .value
                .get("raw_hash")
                .and_then(Value::as_str)
                .map(str::to_string),
        })
    }

    pub fn write_to_fact_kernel<S>(&self, service: &mut FactKernelService<S>) -> FactRecord
    where
        S: FactStore,
    {
        service.upsert_fact(self.to_fact_record())
    }

    #[must_use]
    pub fn to_fact_record(&self) -> FactRecord {
        let kernel = self.to_fact_kernel_matrix_fact();
        let statement = format!("{} {} {}", kernel.entity, kernel.predicate, kernel.value);
        let mut record = FactRecord::new(format!("matrix.{}", kernel.predicate), statement);
        record.id = kernel.id;
        record.confidence = kernel.confidence;
        record.provenance = vec![Provenance {
            source: kernel.source,
            observed_at: self.event_time,
            trace_id: Some(self.snapshot_id.clone()),
        }];
        record.created_at = self.event_time;
        record.updated_at = self.event_time;
        record
    }
}

fn confidence_basis_points(confidence: f32) -> u16 {
    (confidence.clamp(0.0, 1.0) * 10_000.0).round() as u16
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixFactInput {
    #[serde(default)]
    pub fact_id: Option<String>,
    #[serde(default)]
    pub snapshot_id: Option<String>,
    pub fact_type: String,
    #[serde(default)]
    pub entity_refs: Vec<String>,
    #[serde(default)]
    pub metric_key: Option<String>,
    #[serde(default)]
    pub dimensions: Value,
    #[serde(default)]
    pub measures: Value,
    #[serde(default)]
    pub event_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub valid_from: Option<DateTime<Utc>>,
    #[serde(default)]
    pub valid_to: Option<DateTime<Utc>>,
    #[serde(default)]
    pub source_ref: Option<String>,
    #[serde(default)]
    pub confidence: Option<f32>,
    #[serde(default)]
    pub raw_hash: Option<String>,
}

fn stable_hash(value: &Value) -> String {
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    let digest = Sha256::digest(bytes);
    format!("sha256:{digest:x}")
}

#[cfg(test)]
mod fact_kernel_bridge_tests {
    use super::*;

    #[test]
    fn matrix_fact_projects_to_fact_kernel_fact() {
        let fact = MatrixFact::from_input(MatrixFactInput {
            fact_id: Some("fact-1".to_string()),
            snapshot_id: Some("snapshot-1".to_string()),
            fact_type: AI_GROWTH_SIGNAL_FACT.to_string(),
            entity_refs: vec!["agent:coder".to_string()],
            metric_key: Some("success_rate".to_string()),
            dimensions: serde_json::json!({"task": "refactor"}),
            measures: serde_json::json!({"score": 0.91}),
            event_time: Some(Utc::now()),
            valid_from: None,
            valid_to: None,
            source_ref: Some("eval://run-1".to_string()),
            confidence: Some(0.91),
            raw_hash: Some("sha256:test".to_string()),
        });

        let kernel_fact = fact.to_fact_kernel_matrix_fact();

        assert_eq!(kernel_fact.id.as_str(), "fact-1");
        assert_eq!(kernel_fact.entity, "agent:coder");
        assert_eq!(kernel_fact.predicate, AI_GROWTH_SIGNAL_FACT);
        assert_eq!(kernel_fact.confidence.basis_points(), Some(9_100));
        assert_eq!(kernel_fact.source.id, "eval://run-1");
    }

    #[test]
    fn matrix_fact_round_trips_from_fact_kernel_fact() {
        let original = MatrixFact::from_input(MatrixFactInput {
            fact_id: Some("fact-2".to_string()),
            snapshot_id: Some("snapshot-2".to_string()),
            fact_type: AI_EVAL_RESULT_FACT.to_string(),
            entity_refs: vec!["harness:gateway".to_string()],
            metric_key: Some("architecture_health".to_string()),
            dimensions: serde_json::json!({"area": "gateway"}),
            measures: serde_json::json!({"score": 1.0}),
            event_time: Some(Utc::now()),
            valid_from: None,
            valid_to: None,
            source_ref: Some("test://architecture".to_string()),
            confidence: Some(1.0),
            raw_hash: Some("sha256:roundtrip".to_string()),
        });

        let restored =
            MatrixFact::from_fact_kernel_matrix_fact(original.to_fact_kernel_matrix_fact(), "s3");

        assert_eq!(restored.fact_id, original.fact_id);
        assert_eq!(restored.snapshot_id, "s3");
        assert_eq!(restored.fact_type, original.fact_type);
        assert_eq!(restored.entity_refs, original.entity_refs);
        assert_eq!(restored.metric_key, original.metric_key);
        assert_eq!(restored.confidence, original.confidence);
    }

    #[test]
    fn matrix_fact_writes_to_fact_kernel_service_for_recall() {
        let fact = MatrixFact::from_input(MatrixFactInput {
            fact_id: Some("fact-3".to_string()),
            snapshot_id: Some("snapshot-3".to_string()),
            fact_type: AI_EXECUTION_GRAPH_QUALITY_FACT.to_string(),
            entity_refs: vec!["harness:runtime".to_string()],
            metric_key: Some("quality_score".to_string()),
            dimensions: serde_json::json!({"area": "runtime"}),
            measures: serde_json::json!({"score": 0.88}),
            event_time: Some(Utc::now()),
            valid_from: None,
            valid_to: None,
            source_ref: Some("eval://run-3".to_string()),
            confidence: Some(0.88),
            raw_hash: Some("sha256:fact3".to_string()),
        });
        let mut service = FactKernelService::new();

        let record = fact.write_to_fact_kernel(&mut service);
        let hits = service.recall(&fact_kernel::memory::RecallQuery::new("runtime quality"));

        assert_eq!(record.id.as_str(), "fact-3");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].fact.id, record.id);
    }
}
