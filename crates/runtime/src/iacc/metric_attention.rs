use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{IaccComputeJobInput, IaccMetricState};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IaccMetricAttentionScore {
    pub metric_id: String,
    pub score: f32,
    #[serde(default)]
    pub reason_codes: Vec<String>,
    pub business_priority: f32,
    pub dependency_count: usize,
    #[serde(default)]
    pub latest_status: Option<String>,
    #[serde(default)]
    pub latest_delta: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IaccMetricAttentionPlan {
    pub plan_id: String,
    pub trigger_fact_type: String,
    #[serde(default)]
    pub entity_scope: Option<String>,
    #[serde(default)]
    pub period: Option<String>,
    pub limit: usize,
    #[serde(default)]
    pub scored_metrics: Vec<IaccMetricAttentionScore>,
    #[serde(default)]
    pub selected_metric_ids: Vec<String>,
    #[serde(default)]
    pub compute_jobs: Vec<IaccComputeJobInput>,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IaccMetricSnapshotItem {
    pub metric_id: String,
    #[serde(default)]
    pub state: Option<IaccMetricState>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IaccMetricSnapshot {
    pub snapshot_id: String,
    pub scope_ref: String,
    #[serde(default)]
    pub metric_ids: Vec<String>,
    #[serde(default)]
    pub items: Vec<IaccMetricSnapshotItem>,
    pub created_at: DateTime<Utc>,
    pub summary: String,
}

impl IaccMetricAttentionScore {
    #[must_use]
    pub fn new(
        metric_id: impl Into<String>,
        business_priority: f32,
        dependency_count: usize,
        latest_status: Option<String>,
        latest_delta: Option<f64>,
    ) -> Self {
        let metric_id = metric_id.into();
        let mut score = business_priority * 0.45 + (dependency_count as f32).min(8.0) / 8.0 * 0.30;
        let mut reason_codes = vec![
            "business_priority".to_string(),
            "dependency_fanout".to_string(),
        ];
        if let Some(status) = &latest_status {
            match status.as_str() {
                "critical" => {
                    score += 0.20;
                    reason_codes.push("critical_state".to_string());
                }
                "warning" => {
                    score += 0.12;
                    reason_codes.push("warning_state".to_string());
                }
                _ => {}
            }
        }
        if latest_delta.is_some_and(|delta| delta.abs() > 0.0) {
            score += 0.08;
            reason_codes.push("recent_delta".to_string());
        }
        Self {
            metric_id,
            score: score.min(1.0),
            reason_codes,
            business_priority,
            dependency_count,
            latest_status,
            latest_delta,
        }
    }
}

#[must_use]
pub fn build_metric_compute_jobs(
    trigger_fact_type: &str,
    metric_ids: &[String],
    entity_scope: Option<String>,
    period: Option<String>,
) -> Vec<IaccComputeJobInput> {
    metric_ids
        .iter()
        .map(|metric_id| IaccComputeJobInput {
            job_id: Some(format!(
                "compute-job-{}-{}-{}",
                trigger_fact_type.replace('.', "_"),
                metric_id,
                uuid::Uuid::new_v4()
            )),
            trigger_fact_type: trigger_fact_type.to_string(),
            trigger_fact_refs: vec![format!("iacc:metric:{metric_id}")],
            entity_scope: entity_scope.clone(),
            period: period.clone(),
            metric_ids: vec![metric_id.clone()],
            priority: Some(0.85),
        })
        .collect()
}
