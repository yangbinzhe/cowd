use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixMetricDependencyInput {
    #[serde(default)]
    pub dependency_id: Option<String>,
    pub upstream_metric_id: String,
    pub downstream_metric_id: String,
    pub dependency_type: String,
    #[serde(default)]
    pub entity_relation_type: Option<String>,
    #[serde(default)]
    pub required_fact_types: Vec<String>,
    #[serde(default)]
    pub transformation_ref: Option<String>,
    #[serde(default)]
    pub confidence: Option<f32>,
    #[serde(default)]
    pub notes: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixMetricDependency {
    pub dependency_id: String,
    pub upstream_metric_id: String,
    pub downstream_metric_id: String,
    pub dependency_type: String,
    #[serde(default)]
    pub entity_relation_type: Option<String>,
    #[serde(default)]
    pub required_fact_types: Vec<String>,
    #[serde(default)]
    pub transformation_ref: Option<String>,
    pub confidence: f32,
    #[serde(default)]
    pub notes: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl MatrixMetricDependency {
    #[must_use]
    pub fn from_input(input: MatrixMetricDependencyInput) -> Self {
        let now = Utc::now();
        Self {
            dependency_id: input.dependency_id.unwrap_or_else(|| {
                format!(
                    "metric-dependency-{}-{}-{}",
                    input.upstream_metric_id, input.downstream_metric_id, input.dependency_type
                )
            }),
            upstream_metric_id: input.upstream_metric_id,
            downstream_metric_id: input.downstream_metric_id,
            dependency_type: input.dependency_type,
            entity_relation_type: input.entity_relation_type,
            required_fact_types: input.required_fact_types,
            transformation_ref: input.transformation_ref,
            confidence: input.confidence.unwrap_or(0.8),
            notes: input.notes,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixMetricLineage {
    pub metric_id: String,
    #[serde(default)]
    pub upstream_dependencies: Vec<MatrixMetricDependency>,
    #[serde(default)]
    pub downstream_dependencies: Vec<MatrixMetricDependency>,
    #[serde(default)]
    pub impacted_metric_ids: Vec<String>,
    pub generated_at: DateTime<Utc>,
}
