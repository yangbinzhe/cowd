use chrono::{DateTime, Utc};
use memory::{RuntimeEvent, RuntimeEventScope, RuntimeRef};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CowdExecutionOutcomeKind {
    Tool,
    Agent,
    Task,
    StructuredIngest,
    StructuredFact,
    StructuredEvidence,
    ApplicationCompute,
    ApplicationAction,
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

    #[test]
    fn ingest_plan_outcome_preserves_structured_refs_and_metrics() {
        let outcome = CowdExecutionOutcome {
            outcome_id: "batch-1".to_string(),
            kind: CowdExecutionOutcomeKind::StructuredIngest,
            status: CowdExecutionOutcomeStatus::Planned,
            title: "Ingest inventory balance".to_string(),
            summary: "12 rows planned for partition 2026-W30".to_string(),
            domain: Some("manufacturing".to_string()),
            refs: vec![CowdExecutionRef {
                ref_type: "structured_batch".to_string(),
                id: "batch-1".to_string(),
                label: Some("pack-1".to_string()),
            }],
            evidence_refs: Vec::new(),
            metrics: vec!["stock_on_hand".to_string()],
            payload: serde_json::json!({"source_ref": "pack-1"}),
            created_at: DateTime::<Utc>::UNIX_EPOCH,
        };
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
        let outcome = CowdExecutionOutcome {
            outcome_id: "job-1".to_string(),
            kind: CowdExecutionOutcomeKind::ApplicationCompute,
            status: CowdExecutionOutcomeStatus::Succeeded,
            title: "Compute stock on hand".to_string(),
            summary: "Compute job completed".to_string(),
            domain: Some("manufacturing".to_string()),
            refs: vec![CowdExecutionRef {
                ref_type: "structured_compute_job".to_string(),
                id: "job-1".to_string(),
                label: Some("inventory_balance".to_string()),
            }],
            evidence_refs: vec!["structured-fact:fact-1".to_string()],
            metrics: vec!["stock_on_hand".to_string()],
            payload: serde_json::json!({"period": "2026-W30"}),
            created_at: DateTime::<Utc>::UNIX_EPOCH,
        };

        assert_eq!(outcome.status, CowdExecutionOutcomeStatus::Succeeded);
        assert_eq!(outcome.evidence_refs, vec!["structured-fact:fact-1"]);
        assert_eq!(outcome.metrics, vec!["stock_on_hand"]);
        assert_eq!(outcome.refs[0].ref_type, "structured_compute_job");
    }

    #[test]
    fn structured_fact_outcome_keeps_fact_source_and_metric_refs() {
        let outcome = CowdExecutionOutcome {
            outcome_id: "fact-1".to_string(),
            kind: CowdExecutionOutcomeKind::StructuredFact,
            status: CowdExecutionOutcomeStatus::Succeeded,
            title: "Inventory balance fact".to_string(),
            summary: "Fact persisted".to_string(),
            domain: Some("manufacturing".to_string()),
            refs: vec![
                CowdExecutionRef {
                    ref_type: "structured_fact".to_string(),
                    id: "fact-1".to_string(),
                    label: Some("inventory_balance".to_string()),
                },
                CowdExecutionRef {
                    ref_type: "structured_source".to_string(),
                    id: "pack-1".to_string(),
                    label: None,
                },
            ],
            evidence_refs: Vec::new(),
            metrics: vec!["stock_on_hand".to_string()],
            payload: serde_json::json!({"confidence": 0.95}),
            created_at: DateTime::<Utc>::UNIX_EPOCH,
        };
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
        let outcome = CowdExecutionOutcome {
            outcome_id: "evidence-1".to_string(),
            kind: CowdExecutionOutcomeKind::StructuredEvidence,
            status: CowdExecutionOutcomeStatus::Partial,
            title: "Inventory balance needs review".to_string(),
            summary: "Evidence packet contains one metric signal".to_string(),
            domain: Some("manufacturing".to_string()),
            refs: vec![CowdExecutionRef {
                ref_type: "structured_evidence".to_string(),
                id: "evidence-1".to_string(),
                label: Some("attention-1".to_string()),
            }],
            evidence_refs: vec!["structured-fact:fact-1".to_string()],
            metrics: vec!["stock_on_hand".to_string()],
            payload: serde_json::json!({"metric_id": "stock_on_hand"}),
            created_at: DateTime::<Utc>::UNIX_EPOCH,
        };
        let event = outcome.to_runtime_event("session-1", 3);

        assert_eq!(outcome.kind, CowdExecutionOutcomeKind::StructuredEvidence);
        assert_eq!(outcome.status, CowdExecutionOutcomeStatus::Partial);
        assert_eq!(outcome.metrics, vec!["stock_on_hand"]);
        assert_eq!(outcome.evidence_refs, vec!["structured-fact:fact-1"]);
        assert!(event.refs.iter().any(|reference| {
            reference.ref_type == "structured_evidence" && reference.id == "evidence-1"
        }));
    }
}
