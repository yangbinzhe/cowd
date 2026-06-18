use chrono::{DateTime, Utc};
use memory::{RuntimeEvent, RuntimeEventScope, RuntimeRef};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::matrix::{
    MatrixComputeJob, MatrixDataPlaneIngestPlan, MatrixEvidencePacket, MatrixFact,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CowdExecutionOutcomeKind {
    Tool,
    Agent,
    Task,
    StructuredIngest,
    StructuredFact,
    StructuredEvidence,
    ManufacturingCompute,
    ManufacturingAction,
    SkillRun,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CowdExecutionOutcomeStatus {
    Planned,
    Running,
    Succeeded,
    Failed,
    Blocked,
    Partial,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CowdExecutionRef {
    #[serde(rename = "type")]
    pub ref_type: String,
    pub id: String,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CowdExecutionOutcome {
    pub outcome_id: String,
    pub kind: CowdExecutionOutcomeKind,
    pub status: CowdExecutionOutcomeStatus,
    pub title: String,
    pub summary: String,
    #[serde(default)]
    pub domain: Option<String>,
    #[serde(default)]
    pub refs: Vec<CowdExecutionRef>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub metrics: Vec<String>,
    #[serde(default)]
    pub payload: Value,
    pub created_at: DateTime<Utc>,
}

impl CowdExecutionOutcome {
    #[must_use]
    pub fn to_runtime_event(&self, session_id: impl Into<String>, sequence: usize) -> RuntimeEvent {
        let mut event = RuntimeEvent::new(
            session_id,
            sequence,
            RuntimeEventScope::Task,
            "execution.outcome",
            serde_json::to_value(self).unwrap_or_else(|_| {
                serde_json::json!({
                    "outcome_id": self.outcome_id,
                    "status": format!("{:?}", self.status).to_ascii_lowercase(),
                })
            }),
            created_at_ms(self.created_at),
        );
        event.status = Some(status_label(self.status).to_string());
        event.refs = self
            .refs
            .iter()
            .map(|reference| RuntimeRef {
                ref_type: reference.ref_type.clone(),
                id: reference.id.clone(),
                label: reference.label.clone(),
            })
            .collect();
        event
    }
}

impl From<&MatrixDataPlaneIngestPlan> for CowdExecutionOutcome {
    fn from(plan: &MatrixDataPlaneIngestPlan) -> Self {
        Self {
            outcome_id: format!("structured-ingest:{}", plan.batch_id),
            kind: CowdExecutionOutcomeKind::StructuredIngest,
            status: CowdExecutionOutcomeStatus::Planned,
            title: format!("Structured ingest plan for {}", plan.fact_type),
            summary: format!(
                "Plan {} ingests {} estimated rows from {} partition {}.",
                plan.batch_id, plan.estimated_rows, plan.source_ref, plan.partition_ref
            ),
            domain: Some("matrix".to_string()),
            refs: vec![
                CowdExecutionRef {
                    ref_type: "structured_source".to_string(),
                    id: plan.source_ref.clone(),
                    label: Some(plan.source_ref.clone()),
                },
                CowdExecutionRef {
                    ref_type: "structured_batch".to_string(),
                    id: plan.batch_id.clone(),
                    label: Some(plan.fact_type.clone()),
                },
            ],
            evidence_refs: Vec::new(),
            metrics: plan.affected_metric_ids.clone(),
            payload: serde_json::to_value(plan).unwrap_or(Value::Null),
            created_at: plan.planned_at,
        }
    }
}

impl From<&MatrixComputeJob> for CowdExecutionOutcome {
    fn from(job: &MatrixComputeJob) -> Self {
        Self {
            outcome_id: format!("manufacturing-compute:{}", job.job_id),
            kind: CowdExecutionOutcomeKind::ManufacturingCompute,
            status: status_from_matrix(&job.status),
            title: format!("Manufacturing compute {}", job.trigger_fact_type),
            summary: format!(
                "Compute job {} status {} affects {} metrics.",
                job.job_id,
                job.status,
                job.metric_ids.len()
            ),
            domain: Some("matrix".to_string()),
            refs: vec![CowdExecutionRef {
                ref_type: "matrix_compute_job".to_string(),
                id: job.job_id.clone(),
                label: Some(job.trigger_fact_type.clone()),
            }],
            evidence_refs: job.trigger_fact_refs.clone(),
            metrics: job.metric_ids.clone(),
            payload: serde_json::to_value(job).unwrap_or(Value::Null),
            created_at: job.created_at,
        }
    }
}

impl From<&MatrixFact> for CowdExecutionOutcome {
    fn from(fact: &MatrixFact) -> Self {
        let mut refs = vec![CowdExecutionRef {
            ref_type: "structured_fact".to_string(),
            id: fact.fact_id.clone(),
            label: Some(fact.fact_type.clone()),
        }];
        if let Some(source_ref) = fact.source_ref.as_ref() {
            refs.push(CowdExecutionRef {
                ref_type: "structured_source".to_string(),
                id: source_ref.clone(),
                label: Some(source_ref.clone()),
            });
        }
        Self {
            outcome_id: format!("structured-fact:{}", fact.fact_id),
            kind: CowdExecutionOutcomeKind::StructuredFact,
            status: CowdExecutionOutcomeStatus::Succeeded,
            title: format!("Structured fact {}", fact.fact_type),
            summary: format!(
                "Fact {} of type {} references {} entities with confidence {:.2}.",
                fact.fact_id,
                fact.fact_type,
                fact.entity_refs.len(),
                fact.confidence
            ),
            domain: Some("matrix".to_string()),
            refs,
            evidence_refs: fact
                .source_ref
                .iter()
                .map(|source_ref| format!("structured-source:{source_ref}"))
                .collect(),
            metrics: fact.metric_key.iter().cloned().collect(),
            payload: serde_json::to_value(fact).unwrap_or(Value::Null),
            created_at: fact.event_time,
        }
    }
}

impl From<&MatrixEvidencePacket> for CowdExecutionOutcome {
    fn from(packet: &MatrixEvidencePacket) -> Self {
        Self {
            outcome_id: format!("structured-evidence:{}", packet.packet_id),
            kind: CowdExecutionOutcomeKind::StructuredEvidence,
            status: if packet.missing_evidence.is_empty() {
                CowdExecutionOutcomeStatus::Succeeded
            } else {
                CowdExecutionOutcomeStatus::Partial
            },
            title: format!("Evidence packet {}", packet.packet_id),
            summary: format!(
                "Evidence packet for '{}' has {} metric items, {} change items and confidence {:.2}.",
                packet.problem_statement,
                packet.metric_evidence.len(),
                packet.change_evidence.len(),
                packet.confidence
            ),
            domain: Some("matrix".to_string()),
            refs: vec![CowdExecutionRef {
                ref_type: "structured_evidence".to_string(),
                id: packet.packet_id.clone(),
                label: packet.attention_id.clone(),
            }],
            evidence_refs: packet
                .source_refs
                .iter()
                .map(|source| source.reference.clone())
                .collect(),
            metrics: packet
                .metric_evidence
                .iter()
                .filter_map(|item| item.get("metric_id").and_then(Value::as_str))
                .map(ToString::to_string)
                .collect(),
            payload: serde_json::to_value(packet).unwrap_or(Value::Null),
            created_at: packet.created_at,
        }
    }
}

fn status_from_matrix(status: &str) -> CowdExecutionOutcomeStatus {
    match status {
        "planned" | "dry_run_ready" | "queued_for_human_review" => {
            CowdExecutionOutcomeStatus::Planned
        }
        "running" | "cross_plane_dispatched" => CowdExecutionOutcomeStatus::Running,
        "completed" | "success" | "feedback_resolved" => CowdExecutionOutcomeStatus::Succeeded,
        "failed" | "error" | "feedback_rejected" => CowdExecutionOutcomeStatus::Failed,
        "blocked" | "cross_plane_blocked" => CowdExecutionOutcomeStatus::Blocked,
        _ => CowdExecutionOutcomeStatus::Partial,
    }
}

fn status_label(status: CowdExecutionOutcomeStatus) -> &'static str {
    match status {
        CowdExecutionOutcomeStatus::Planned => "planned",
        CowdExecutionOutcomeStatus::Running => "running",
        CowdExecutionOutcomeStatus::Succeeded => "succeeded",
        CowdExecutionOutcomeStatus::Failed => "failed",
        CowdExecutionOutcomeStatus::Blocked => "blocked",
        CowdExecutionOutcomeStatus::Partial => "partial",
    }
}

fn created_at_ms(created_at: DateTime<Utc>) -> u64 {
    u64::try_from(created_at.timestamp_millis()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matrix::{
        MatrixComputeJob, MatrixComputeJobInput, MatrixDataPlaneIngestPlan,
        MatrixDataPlaneWatermark, MatrixEvidencePacket, MatrixEvidenceSourceRef, MatrixFact,
        MatrixFactInput,
    };

    #[test]
    fn ingest_plan_outcome_preserves_structured_refs_and_metrics() {
        let plan = MatrixDataPlaneIngestPlan {
            batch_id: "batch-1".to_string(),
            source_ref: "pack-1".to_string(),
            fact_type: "inventory_balance".to_string(),
            partition_ref: "2026-W30".to_string(),
            idempotency_key: "idem-1".to_string(),
            replay_policy: "replace_partition_by_idempotency_key".to_string(),
            estimated_rows: 12,
            affected_metric_ids: vec!["stock_on_hand".to_string()],
            compute_jobs: Vec::new(),
            watermark: MatrixDataPlaneWatermark {
                source_ref: "pack-1".to_string(),
                fact_type: "inventory_balance".to_string(),
                partition_ref: "2026-W30".to_string(),
                high_watermark: "2026-06-14T00:00:00Z".to_string(),
                last_batch_id: "batch-1".to_string(),
                updated_at: DateTime::<Utc>::UNIX_EPOCH,
            },
            planned_at: DateTime::<Utc>::UNIX_EPOCH,
        };

        let outcome = CowdExecutionOutcome::from(&plan);
        let event = outcome.to_runtime_event("session-1", 7);

        assert_eq!(outcome.kind, CowdExecutionOutcomeKind::StructuredIngest);
        assert_eq!(outcome.metrics, vec!["stock_on_hand"]);
        assert!(
            outcome
                .refs
                .iter()
                .any(|reference| reference.ref_type == "structured_batch"
                    && reference.id == "batch-1")
        );
        assert_eq!(event.scope, RuntimeEventScope::Task);
        assert_eq!(event.kind, "execution.outcome");
        assert_eq!(event.status.as_deref(), Some("planned"));
    }

    #[test]
    fn compute_job_outcome_maps_status_and_evidence_refs() {
        let mut job = MatrixComputeJob::from_input(MatrixComputeJobInput {
            job_id: Some("job-1".to_string()),
            trigger_fact_type: "inventory_balance".to_string(),
            trigger_fact_refs: vec!["fact-1".to_string()],
            entity_scope: None,
            period: Some("2026-W30".to_string()),
            metric_ids: vec!["stock_on_hand".to_string()],
            priority: Some(0.8),
        });
        job.status = "completed".to_string();

        let outcome = CowdExecutionOutcome::from(&job);

        assert_eq!(outcome.status, CowdExecutionOutcomeStatus::Succeeded);
        assert_eq!(outcome.evidence_refs, vec!["fact-1"]);
        assert_eq!(outcome.metrics, vec!["stock_on_hand"]);
        assert_eq!(outcome.refs[0].ref_type, "matrix_compute_job");
    }

    #[test]
    fn structured_fact_outcome_keeps_fact_source_and_metric_refs() {
        let fact = MatrixFact::from_input(MatrixFactInput {
            fact_id: Some("fact-1".to_string()),
            snapshot_id: Some("snapshot-1".to_string()),
            fact_type: "inventory_balance".to_string(),
            entity_refs: vec!["factory:sz".to_string()],
            metric_key: Some("stock_on_hand".to_string()),
            dimensions: serde_json::json!({"week": "2026-W30"}),
            measures: serde_json::json!({"qty": 42}),
            event_time: Some(DateTime::<Utc>::UNIX_EPOCH),
            valid_from: None,
            valid_to: None,
            source_ref: Some("pack-1".to_string()),
            confidence: Some(0.95),
            raw_hash: Some("sha256:fact".to_string()),
        });

        let outcome = CowdExecutionOutcome::from(&fact);
        let event = outcome.to_runtime_event("session-1", 2);

        assert_eq!(outcome.kind, CowdExecutionOutcomeKind::StructuredFact);
        assert_eq!(outcome.status, CowdExecutionOutcomeStatus::Succeeded);
        assert_eq!(outcome.metrics, vec!["stock_on_hand"]);
        assert!(event.refs.iter().any(|reference| {
            reference.ref_type == "structured_fact" && reference.id == "fact-1"
        }));
        assert!(event.refs.iter().any(|reference| {
            reference.ref_type == "structured_source" && reference.id == "pack-1"
        }));
    }

    #[test]
    fn structured_evidence_outcome_keeps_packet_refs_and_partial_status() {
        let mut packet = MatrixEvidencePacket::new("Inventory balance needs review");
        packet.packet_id = "evidence-1".to_string();
        packet.attention_id = Some("attention-1".to_string());
        packet.metric_evidence = vec![serde_json::json!({"metric_id": "stock_on_hand"})];
        packet.source_refs = vec![MatrixEvidenceSourceRef {
            kind: "fact".to_string(),
            reference: "matrix:fact:fact-1".to_string(),
            summary: "Fact source".to_string(),
        }];

        let outcome = CowdExecutionOutcome::from(&packet);
        let event = outcome.to_runtime_event("session-1", 3);

        assert_eq!(outcome.kind, CowdExecutionOutcomeKind::StructuredEvidence);
        assert_eq!(outcome.status, CowdExecutionOutcomeStatus::Partial);
        assert_eq!(outcome.metrics, vec!["stock_on_hand"]);
        assert_eq!(outcome.evidence_refs, vec!["matrix:fact:fact-1"]);
        assert!(event.refs.iter().any(|reference| {
            reference.ref_type == "structured_evidence" && reference.id == "evidence-1"
        }));
    }
}
