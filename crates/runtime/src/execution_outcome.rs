use chrono::{DateTime, Utc};
use memory::{RuntimeEvent, RuntimeEventScope, RuntimeRef};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::iacc::{IaccActionExecution, IaccComputeJob, IaccDataPlaneIngestPlan, IaccSkillRun};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CowdExecutionOutcomeKind {
    Tool,
    Agent,
    Task,
    StructuredIngest,
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

impl From<&IaccDataPlaneIngestPlan> for CowdExecutionOutcome {
    fn from(plan: &IaccDataPlaneIngestPlan) -> Self {
        Self {
            outcome_id: format!("structured-ingest:{}", plan.batch_id),
            kind: CowdExecutionOutcomeKind::StructuredIngest,
            status: CowdExecutionOutcomeStatus::Planned,
            title: format!("Structured ingest plan for {}", plan.fact_type),
            summary: format!(
                "Plan {} ingests {} estimated rows from {} partition {}.",
                plan.batch_id, plan.estimated_rows, plan.source_ref, plan.partition_ref
            ),
            domain: Some("iacc".to_string()),
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

impl From<&IaccComputeJob> for CowdExecutionOutcome {
    fn from(job: &IaccComputeJob) -> Self {
        Self {
            outcome_id: format!("manufacturing-compute:{}", job.job_id),
            kind: CowdExecutionOutcomeKind::ManufacturingCompute,
            status: status_from_iacc(&job.status),
            title: format!("Manufacturing compute {}", job.trigger_fact_type),
            summary: format!(
                "Compute job {} status {} affects {} metrics.",
                job.job_id,
                job.status,
                job.metric_ids.len()
            ),
            domain: Some("iacc".to_string()),
            refs: vec![CowdExecutionRef {
                ref_type: "iacc_compute_job".to_string(),
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

impl From<&IaccActionExecution> for CowdExecutionOutcome {
    fn from(execution: &IaccActionExecution) -> Self {
        Self {
            outcome_id: format!("manufacturing-action:{}", execution.execution_id),
            kind: CowdExecutionOutcomeKind::ManufacturingAction,
            status: status_from_iacc(&execution.status),
            title: execution.title.clone(),
            summary: format!(
                "IACC action {} for incident {} is {}.",
                execution.action_id, execution.incident_id, execution.status
            ),
            domain: Some("iacc".to_string()),
            refs: vec![
                CowdExecutionRef {
                    ref_type: "iacc_execution".to_string(),
                    id: execution.execution_id.clone(),
                    label: Some(execution.action_type.clone()),
                },
                CowdExecutionRef {
                    ref_type: "iacc_incident".to_string(),
                    id: execution.incident_id.clone(),
                    label: None,
                },
            ],
            evidence_refs: execution
                .cross_plane_receipts
                .iter()
                .filter_map(|receipt| receipt.audit_record_id.clone())
                .collect(),
            metrics: Vec::new(),
            payload: execution.receipt.clone(),
            created_at: execution.created_at,
        }
    }
}

impl From<&IaccSkillRun> for CowdExecutionOutcome {
    fn from(run: &IaccSkillRun) -> Self {
        Self {
            outcome_id: format!(
                "skill-run:{}",
                run.execution_id
                    .clone()
                    .unwrap_or_else(|| format!("{}:{}", run.incident_id, run.skill_id))
            ),
            kind: CowdExecutionOutcomeKind::SkillRun,
            status: status_from_iacc(&run.status),
            title: format!("Skill run {}", run.skill_id),
            summary: run.summary.clone(),
            domain: Some("iacc".to_string()),
            refs: vec![
                CowdExecutionRef {
                    ref_type: "iacc_skill".to_string(),
                    id: run.skill_id.clone(),
                    label: run.agent_node_id.clone(),
                },
                CowdExecutionRef {
                    ref_type: "iacc_incident".to_string(),
                    id: run.incident_id.clone(),
                    label: None,
                },
            ],
            evidence_refs: run
                .execution_context
                .as_ref()
                .map(|context| context.evidence_refs.clone())
                .unwrap_or_default(),
            metrics: run
                .execution_context
                .as_ref()
                .map(|context| context.metric_keys.clone())
                .unwrap_or_default(),
            payload: run.structured_report.clone(),
            created_at: run
                .telemetry
                .as_ref()
                .map(|telemetry| telemetry.completed_at)
                .unwrap_or_else(Utc::now),
        }
    }
}

fn status_from_iacc(status: &str) -> CowdExecutionOutcomeStatus {
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
    use crate::iacc::{
        IaccComputeJob, IaccComputeJobInput, IaccDataPlaneIngestPlan, IaccDataPlaneWatermark,
    };

    #[test]
    fn ingest_plan_outcome_preserves_structured_refs_and_metrics() {
        let plan = IaccDataPlaneIngestPlan {
            batch_id: "batch-1".to_string(),
            source_ref: "pack-1".to_string(),
            fact_type: "inventory_balance".to_string(),
            partition_ref: "2026-W30".to_string(),
            idempotency_key: "idem-1".to_string(),
            replay_policy: "replace_partition_by_idempotency_key".to_string(),
            estimated_rows: 12,
            affected_metric_ids: vec!["stock_on_hand".to_string()],
            compute_jobs: Vec::new(),
            watermark: IaccDataPlaneWatermark {
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
        let mut job = IaccComputeJob::from_input(IaccComputeJobInput {
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
        assert_eq!(outcome.refs[0].ref_type, "iacc_compute_job");
    }
}
