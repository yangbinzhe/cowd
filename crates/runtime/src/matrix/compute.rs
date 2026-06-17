use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixComputeJobInput {
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
pub struct MatrixComputeJob {
    pub job_id: String,
    pub trigger_fact_type: String,
    #[serde(default)]
    pub trigger_fact_refs: Vec<String>,
    #[serde(default)]
    pub entity_scope: Option<String>,
    #[serde(default)]
    pub period: Option<String>,
    #[serde(default)]
    pub metric_ids: Vec<String>,
    pub priority: f32,
    pub status: String,
    pub attempts: u32,
    #[serde(default)]
    pub result_summary: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl MatrixComputeJob {
    #[must_use]
    pub fn from_input(input: MatrixComputeJobInput) -> Self {
        let now = Utc::now();
        Self {
            job_id: input
                .job_id
                .unwrap_or_else(|| format!("compute-job-{}", uuid::Uuid::new_v4())),
            trigger_fact_type: input.trigger_fact_type,
            trigger_fact_refs: input.trigger_fact_refs,
            entity_scope: input.entity_scope,
            period: input.period,
            metric_ids: input.metric_ids,
            priority: input.priority.unwrap_or(0.5),
            status: "planned".to_string(),
            attempts: 0,
            result_summary: Value::Null,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixComputePlan {
    pub job: MatrixComputeJob,
    #[serde(default)]
    pub affected_metric_ids: Vec<String>,
    pub planned_at: DateTime<Utc>,
}
