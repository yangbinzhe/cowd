//! Stable, transport-neutral presentation contracts shared by APP producers
//! and Cowd surfaces.  Domain applications own meaning; this module only owns
//! the small set of shapes and capability declarations required to render it.

use std::collections::{BTreeMap, BTreeSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{AppContractError, AppId};

pub const PRESENTATION_SCHEMA_VERSION: u16 = 1;
pub const PRESENTATION_SCHEMA_ID: &str = "cowd.presentation.result-shape.v1";

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ResultShapeKind {
    Scalar,
    Series,
    Table,
    Matrix,
    Graph,
    Timeline,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum ResultValue {
    Null(()),
    Bool(bool),
    Number(f64),
    Text(String),
}

impl ResultValue {
    #[must_use]
    pub fn from_json(value: &serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Self::Null(()),
            serde_json::Value::Bool(value) => Self::Bool(*value),
            serde_json::Value::Number(value) => value
                .as_f64()
                .map_or_else(|| Self::Text(value.to_string()), Self::Number),
            serde_json::Value::String(value) => Self::Text(value.clone()),
            value => Self::Text(value.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ResultEvidence {
    pub source_ref: String,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ScalarResult {
    pub value: ResultValue,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub unit: Option<String>,
    #[serde(default)]
    pub change: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SeriesPoint {
    pub x: String,
    pub y: f64,
    #[serde(default)]
    pub series: Option<String>,
    #[serde(default)]
    pub source_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SeriesResult {
    pub points: Vec<SeriesPoint>,
    #[serde(default)]
    pub x_label: Option<String>,
    #[serde(default)]
    pub y_label: Option<String>,
    #[serde(default)]
    pub unit: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TableValueKind {
    Text,
    Number,
    Boolean,
    Timestamp,
    Status,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TableColumn {
    pub key: String,
    pub label: String,
    pub value_kind: TableValueKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TableRow {
    pub id: String,
    pub cells: BTreeMap<String, ResultValue>,
    #[serde(default)]
    pub source_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TableResult {
    pub columns: Vec<TableColumn>,
    pub rows: Vec<TableRow>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MatrixCell {
    pub x: String,
    pub y: String,
    pub value: f64,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub source_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MatrixResult {
    pub x_labels: Vec<String>,
    pub y_labels: Vec<String>,
    pub cells: Vec<MatrixCell>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct GraphNode {
    pub id: String,
    pub label: String,
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub source_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GraphEdge {
    pub source: String,
    pub target: String,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct GraphResult {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TimelineItem {
    pub id: String,
    pub at: String,
    pub title: String,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub source_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TimelineResult {
    pub items: Vec<TimelineItem>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "content", rename_all = "snake_case")]
pub enum ResultShape {
    Scalar(ScalarResult),
    Series(SeriesResult),
    Table(TableResult),
    Matrix(MatrixResult),
    Graph(GraphResult),
    Timeline(TimelineResult),
}

impl ResultShape {
    #[must_use]
    pub const fn kind(&self) -> ResultShapeKind {
        match self {
            Self::Scalar(_) => ResultShapeKind::Scalar,
            Self::Series(_) => ResultShapeKind::Series,
            Self::Table(_) => ResultShapeKind::Table,
            Self::Matrix(_) => ResultShapeKind::Matrix,
            Self::Graph(_) => ResultShapeKind::Graph,
            Self::Timeline(_) => ResultShapeKind::Timeline,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AppRendererContract {
    pub renderer_id: String,
    pub renderer_version: u32,
    pub accepted_shapes: Vec<ResultShapeKind>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AppResultContract {
    pub contract_id: String,
    pub schema_id: String,
    pub schema_version: u16,
    pub schema_digest: String,
    pub max_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AppPresentationContribution {
    pub result_contracts: Vec<AppResultContract>,
    pub renderers: Vec<AppRendererContract>,
}

impl AppPresentationContribution {
    pub fn validate_for(&self, app_id: &AppId) -> Result<(), AppContractError> {
        let mut contracts = BTreeSet::new();
        for contract in &self.result_contracts {
            if contract.contract_id.trim().is_empty()
                || contract.schema_id.trim().is_empty()
                || contract.schema_version == 0
                || contract.schema_digest.len() != 64
                || !contract
                    .schema_digest
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
                || contract.max_bytes == 0
                || !contracts.insert(contract.contract_id.as_str())
            {
                return Err(AppContractError::InvalidPresentationContribution {
                    app_id: app_id.clone(),
                    reason: "result contract id, version, digest or size limit is invalid"
                        .to_string(),
                });
            }
        }
        let mut renderers = BTreeSet::new();
        for renderer in &self.renderers {
            if renderer.renderer_id.trim().is_empty()
                || renderer.renderer_version == 0
                || renderer.accepted_shapes.is_empty()
                || !renderers.insert(renderer.renderer_id.as_str())
            {
                return Err(AppContractError::InvalidPresentationContribution {
                    app_id: app_id.clone(),
                    reason: "renderer id, version or accepted shape set is invalid".to_string(),
                });
            }
        }
        Ok(())
    }
}

#[must_use]
pub fn result_shape_schema() -> schemars::Schema {
    schemars::schema_for!(ResultShape)
}

#[must_use]
#[allow(clippy::expect_used)]
pub fn result_shape_schema_digest() -> String {
    let bytes = serde_json::to_vec(&result_shape_schema())
        .expect("ResultShape JSON schema is always serializable");
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_shape_schema_has_stable_identity_and_all_six_shapes() {
        let schema = serde_json::to_string(&result_shape_schema()).expect("schema");
        assert_eq!(result_shape_schema_digest().len(), 64);
        for kind in ["scalar", "series", "table", "matrix", "graph", "timeline"] {
            assert!(schema.contains(kind), "missing {kind} shape");
        }
    }

    #[test]
    fn presentation_contribution_rejects_duplicate_or_unbounded_contracts() {
        let app_id = AppId::parse("fixture").expect("app id");
        let contribution = AppPresentationContribution {
            result_contracts: vec![AppResultContract {
                contract_id: "fixture.result.v1".to_string(),
                schema_id: PRESENTATION_SCHEMA_ID.to_string(),
                schema_version: PRESENTATION_SCHEMA_VERSION,
                schema_digest: result_shape_schema_digest(),
                max_bytes: 256 * 1024,
            }],
            renderers: vec![AppRendererContract {
                renderer_id: "table".to_string(),
                renderer_version: 1,
                accepted_shapes: vec![ResultShapeKind::Table],
            }],
        };
        contribution.validate_for(&app_id).expect("valid contract");

        let mut invalid = contribution;
        invalid.result_contracts[0].max_bytes = 0;
        assert!(matches!(
            invalid.validate_for(&app_id),
            Err(AppContractError::InvalidPresentationContribution { .. })
        ));
    }
}
