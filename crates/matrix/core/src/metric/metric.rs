use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    MatrixAggregation, MatrixQueryPlan, MATRIX_FORMULA_SUM_V1, MATRIX_QUERY_PLAN_SCHEMA_V1,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MatrixMetricStatus {
    Normal,
    Warning,
    Critical,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixMetricDefinition {
    pub metric_id: String,
    pub name: String,
    pub domain: String,
    pub grain: String,
    pub owner_role: String,
    pub formula_ref: String,
    #[serde(default = "default_metric_measure")]
    pub measure: String,
    #[serde(default)]
    pub denominator_measure: Option<String>,
    #[serde(default)]
    pub inputs: Vec<String>,
    #[serde(default)]
    pub dimensions: Vec<String>,
    pub refresh_policy: String,
    #[serde(default)]
    pub threshold_policy: Value,
    #[serde(default)]
    pub dependency_metric_ids: Vec<String>,
    pub business_priority: f32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl MatrixMetricDefinition {
    #[must_use]
    pub fn inferred(metric_id: impl Into<String>, fact_type: impl AsRef<str>) -> Self {
        let metric_id = metric_id.into();
        let now = Utc::now();
        Self {
            name: metric_id.replace('_', " "),
            domain: fact_type
                .as_ref()
                .split('.')
                .next()
                .unwrap_or("operations")
                .to_string(),
            grain: "entity_period".to_string(),
            owner_role: "operations_analyst".to_string(),
            formula_ref: MATRIX_FORMULA_SUM_V1.to_string(),
            measure: "value".to_string(),
            denominator_measure: None,
            inputs: vec![fact_type.as_ref().to_string()],
            dimensions: vec!["entity_ref".to_string(), "period".to_string()],
            refresh_policy: "manual_recompute".to_string(),
            threshold_policy: Value::Null,
            dependency_metric_ids: Vec::new(),
            business_priority: 0.5,
            metric_id,
            created_at: now,
            updated_at: now,
        }
    }

    #[must_use]
    pub fn inferred_for_measure(
        metric_id: impl Into<String>,
        fact_type: impl AsRef<str>,
        measure: impl Into<String>,
    ) -> Self {
        let mut definition = Self::inferred(metric_id, fact_type);
        definition.measure = measure.into();
        definition
    }

    #[must_use]
    pub fn query_plan(&self) -> MatrixQueryPlan {
        MatrixQueryPlan {
            schema_version: MATRIX_QUERY_PLAN_SCHEMA_V1.to_string(),
            metric_id: self.metric_id.clone(),
            formula_ref: self.formula_ref.clone(),
            numerator_measure: self.measure.clone(),
            denominator_measure: self.denominator_measure.clone(),
            aggregation: MatrixAggregation::Sum,
            grain: self.grain.clone(),
            dimensions: self.dimensions.clone(),
            cardinality_limit: 10_000,
        }
    }
}

fn default_metric_measure() -> String {
    "value".to_string()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixMetricState {
    pub state_id: String,
    pub metric_id: String,
    pub entity_scope: String,
    pub period: String,
    pub value: f64,
    #[serde(default)]
    pub previous_value: Option<f64>,
    pub delta: f64,
    #[serde(default)]
    pub delta_ratio: Option<f64>,
    pub status: MatrixMetricStatus,
    pub computed_at: DateTime<Utc>,
    #[serde(default)]
    pub input_fact_refs: Vec<String>,
    pub confidence: f32,
}

impl MatrixMetricState {
    #[must_use]
    pub fn status_for_delta(delta: f64) -> MatrixMetricStatus {
        if delta.abs() >= 100.0 {
            MatrixMetricStatus::Critical
        } else if delta.abs() > 0.0 {
            MatrixMetricStatus::Warning
        } else {
            MatrixMetricStatus::Normal
        }
    }
}
