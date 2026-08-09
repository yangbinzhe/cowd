use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MATRIX_QUERY_PLAN_SCHEMA_V1: &str = "matrix.query-plan.v1";
pub const MATRIX_FORMULA_SUM_V1: &str = "matrix://formula/sum/v1";
pub const MATRIX_FORMULA_RATIO_PERCENT_V1: &str = "matrix://formula/ratio-percent/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MatrixFormulaKind {
    Sum,
    RatioPercent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixFormulaDefinition {
    pub formula_ref: String,
    pub version: u32,
    pub kind: MatrixFormulaKind,
    pub denominator_required: bool,
}

#[must_use]
pub fn matrix_formula_registry() -> Vec<MatrixFormulaDefinition> {
    vec![
        MatrixFormulaDefinition {
            formula_ref: MATRIX_FORMULA_SUM_V1.to_string(),
            version: 1,
            kind: MatrixFormulaKind::Sum,
            denominator_required: false,
        },
        MatrixFormulaDefinition {
            formula_ref: MATRIX_FORMULA_RATIO_PERCENT_V1.to_string(),
            version: 1,
            kind: MatrixFormulaKind::RatioPercent,
            denominator_required: true,
        },
    ]
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum MatrixAggregation {
    #[default]
    Sum,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixQueryPlan {
    pub schema_version: String,
    pub metric_id: String,
    pub formula_ref: String,
    pub numerator_measure: String,
    #[serde(default)]
    pub denominator_measure: Option<String>,
    #[serde(default)]
    pub aggregation: MatrixAggregation,
    pub grain: String,
    #[serde(default)]
    pub dimensions: Vec<String>,
    pub cardinality_limit: usize,
}

impl MatrixQueryPlan {
    pub fn validate(&self) -> Result<(), MatrixQueryPlanError> {
        if self.schema_version != MATRIX_QUERY_PLAN_SCHEMA_V1 {
            return Err(MatrixQueryPlanError::new(format!(
                "unsupported query plan schema: {}",
                self.schema_version
            )));
        }
        if self.metric_id.trim().is_empty() || self.grain.trim().is_empty() {
            return Err(MatrixQueryPlanError::new(
                "metric_id and grain must be non-empty",
            ));
        }
        validate_measure_name(&self.numerator_measure)?;
        if let Some(measure) = self.denominator_measure.as_deref() {
            validate_measure_name(measure)?;
        }
        if self.dimensions.len() > 8 {
            return Err(MatrixQueryPlanError::new(
                "query plan may contain at most eight dimensions",
            ));
        }
        let dimensions = self
            .dimensions
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        if dimensions
            .iter()
            .any(|dimension| !matches!(*dimension, "entity_ref" | "period" | "week"))
            || !dimensions.contains(&"entity_ref")
            || (!dimensions.contains(&"period") && !dimensions.contains(&"week"))
        {
            return Err(MatrixQueryPlanError::new(
                "v1 query plans require entity_ref plus period or week dimensions",
            ));
        }
        if !(1..=10_000).contains(&self.cardinality_limit) {
            return Err(MatrixQueryPlanError::new(
                "cardinality_limit must be between 1 and 10000",
            ));
        }
        let formula = resolve_matrix_formula(&self.formula_ref).ok_or_else(|| {
            MatrixQueryPlanError::new(format!("formula is not registered: {}", self.formula_ref))
        })?;
        if formula.denominator_required != self.denominator_measure.is_some() {
            return Err(MatrixQueryPlanError::new(format!(
                "formula {} denominator contract is not satisfied",
                self.formula_ref
            )));
        }
        Ok(())
    }

    pub fn fingerprint(
        &self,
        principal_scope_digest: &str,
        watermark: &str,
        result_schema_version: &str,
    ) -> Result<String, MatrixQueryPlanError> {
        self.validate()?;
        let mut normalized = self.clone();
        normalized.dimensions.sort();
        normalized.dimensions.dedup();
        let bytes = serde_json::to_vec(&(
            normalized,
            principal_scope_digest,
            watermark,
            result_schema_version,
        ))
        .map_err(|error| MatrixQueryPlanError::new(error.to_string()))?;
        Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatrixQueryInput {
    pub fact_ref: String,
    pub fact_type: String,
    pub metric_id: String,
    pub entity_scope: String,
    pub period: String,
    pub numerator: f64,
    pub denominator: Option<f64>,
    pub confidence: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MatrixQueryResult {
    pub metric_id: String,
    pub entity_scope: String,
    pub period: String,
    pub fact_type: String,
    pub value: f64,
    #[serde(default)]
    pub input_fact_refs: Vec<String>,
    pub confidence: f32,
}

#[derive(Default)]
struct QueryAccumulator {
    fact_type: String,
    numerator: f64,
    denominator: f64,
    input_fact_refs: Vec<String>,
    confidence_sum: f32,
    input_count: usize,
}

pub fn execute_matrix_query_plan(
    plan: &MatrixQueryPlan,
    inputs: impl IntoIterator<Item = MatrixQueryInput>,
) -> Result<Vec<MatrixQueryResult>, MatrixQueryPlanError> {
    plan.validate()?;
    let mut groups = BTreeMap::<(String, String), QueryAccumulator>::new();
    for input in inputs {
        if input.metric_id != plan.metric_id {
            continue;
        }
        if !input.numerator.is_finite()
            || input.denominator.is_some_and(|value| !value.is_finite())
            || !input.confidence.is_finite()
        {
            return Err(MatrixQueryPlanError::new(
                "query input contains a non-finite numeric value",
            ));
        }
        let accumulator = groups
            .entry((input.entity_scope, input.period))
            .or_default();
        if accumulator.fact_type.is_empty() {
            accumulator.fact_type = input.fact_type;
        }
        accumulator.numerator += input.numerator;
        accumulator.denominator += input.denominator.unwrap_or_default();
        accumulator.input_fact_refs.push(input.fact_ref);
        accumulator.confidence_sum += input.confidence;
        accumulator.input_count += 1;
        if groups.len() > plan.cardinality_limit {
            return Err(MatrixQueryPlanError::new(format!(
                "query cardinality exceeds {} groups",
                plan.cardinality_limit
            )));
        }
    }
    groups
        .into_iter()
        .map(|((entity_scope, period), accumulator)| {
            let value = evaluate_matrix_formula(
                plan,
                accumulator.numerator,
                plan.denominator_measure
                    .as_ref()
                    .map(|_| accumulator.denominator),
            )?;
            Ok(MatrixQueryResult {
                metric_id: plan.metric_id.clone(),
                entity_scope,
                period,
                fact_type: accumulator.fact_type,
                value,
                input_fact_refs: accumulator.input_fact_refs,
                confidence: if accumulator.input_count == 0 {
                    0.0
                } else {
                    accumulator.confidence_sum / accumulator.input_count as f32
                },
            })
        })
        .collect()
}

pub fn evaluate_matrix_formula(
    plan: &MatrixQueryPlan,
    numerator_sum: f64,
    denominator_sum: Option<f64>,
) -> Result<f64, MatrixQueryPlanError> {
    let formula = resolve_matrix_formula(&plan.formula_ref).ok_or_else(|| {
        MatrixQueryPlanError::new(format!("formula is not registered: {}", plan.formula_ref))
    })?;
    match formula.kind {
        MatrixFormulaKind::Sum => Ok(numerator_sum),
        MatrixFormulaKind::RatioPercent => {
            let denominator = denominator_sum
                .ok_or_else(|| MatrixQueryPlanError::new("ratio formula requires a denominator"))?;
            if denominator.abs() <= f64::EPSILON {
                return Err(MatrixQueryPlanError::new(
                    "ratio formula denominator must be non-zero",
                ));
            }
            Ok(numerator_sum / denominator * 100.0)
        }
    }
}

fn resolve_matrix_formula(formula_ref: &str) -> Option<MatrixFormulaDefinition> {
    matrix_formula_registry()
        .into_iter()
        .find(|formula| formula.formula_ref == formula_ref)
}

fn validate_measure_name(measure: &str) -> Result<(), MatrixQueryPlanError> {
    if measure.is_empty()
        || measure.len() > 128
        || !measure
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(MatrixQueryPlanError::new(format!(
            "invalid top-level measure name: {measure}"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatrixQueryPlanError {
    message: String,
}

impl MatrixQueryPlanError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for MatrixQueryPlanError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MatrixQueryPlanError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn ratio_plan() -> MatrixQueryPlan {
        MatrixQueryPlan {
            schema_version: MATRIX_QUERY_PLAN_SCHEMA_V1.to_string(),
            metric_id: "work_center_load".to_string(),
            formula_ref: MATRIX_FORMULA_RATIO_PERCENT_V1.to_string(),
            numerator_measure: "load_hours".to_string(),
            denominator_measure: Some("capacity_hours".to_string()),
            aggregation: MatrixAggregation::Sum,
            grain: "entity_week".to_string(),
            dimensions: vec!["week".to_string(), "entity_ref".to_string()],
            cardinality_limit: 32,
        }
    }

    #[test]
    fn ratio_formula_uses_explicit_operands_instead_of_summing_json_numbers() {
        let rows = execute_matrix_query_plan(
            &ratio_plan(),
            [MatrixQueryInput {
                fact_ref: "matrix:fact:load-1".to_string(),
                fact_type: "manufacturing.work_center_load".to_string(),
                metric_id: "work_center_load".to_string(),
                entity_scope: "work-center:one".to_string(),
                period: "2026-W30".to_string(),
                numerator: 188.0,
                denominator: Some(160.0),
                confidence: 0.9,
            }],
        )
        .expect("query executes");
        assert_eq!(rows.len(), 1);
        assert!((rows[0].value - 117.5).abs() < f64::EPSILON);
    }

    #[test]
    fn fingerprint_separates_authorization_and_watermark() {
        let plan = ratio_plan();
        let left = plan.fingerprint("scope-a", "wm-1", "shape-v1").unwrap();
        let right = plan.fingerprint("scope-b", "wm-1", "shape-v1").unwrap();
        let advanced = plan.fingerprint("scope-a", "wm-2", "shape-v1").unwrap();
        assert_ne!(left, right);
        assert_ne!(left, advanced);
    }
}
