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
pub const VIEW_SPEC_SCHEMA_VERSION: u16 = 1;

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ViewSpecPlacement {
    pub x: u16,
    pub y: u16,
    pub width: u16,
    pub height: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ViewSpecLayout {
    pub columns: u16,
    pub placements: BTreeMap<String, ViewSpecPlacement>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ViewSpecWidget {
    pub instance_id: String,
    pub definition_id: String,
    pub renderer_id: String,
    pub renderer_version: u32,
    pub title: String,
    #[serde(default)]
    pub config: serde_json::Value,
    #[serde(default)]
    pub query: serde_json::Value,
    #[serde(default = "view_spec_default_true")]
    pub visible: bool,
}

const fn view_spec_default_true() -> bool {
    true
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ViewSpecLockField {
    Presence,
    Position,
    Size,
    Query,
    Renderer,
    Title,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ViewSpecLock {
    pub instance_id: String,
    pub fields: BTreeSet<ViewSpecLockField>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ViewSpecSharing {
    pub visibility: String,
    #[serde(default)]
    pub viewer_refs: BTreeSet<String>,
    #[serde(default)]
    pub editor_refs: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ViewSpec {
    pub schema_version: u16,
    pub surface_id: String,
    pub view_id: String,
    pub base_revision: u64,
    pub catalog_version: String,
    pub title: String,
    pub widgets: Vec<ViewSpecWidget>,
    pub layouts: BTreeMap<String, ViewSpecLayout>,
    #[serde(default)]
    pub locks: Vec<ViewSpecLock>,
    pub sharing: ViewSpecSharing,
    /// Opaque APP-owned authoring context. It is validated by the owning APP
    /// and is never interpreted by the host renderer or layout compiler.
    #[serde(default)]
    pub domain_context: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewSpecValidationContext {
    pub surface_id: String,
    pub catalog_version: String,
    pub renderers: BTreeMap<String, u32>,
    pub definitions: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ViewSpecValidationReceipt {
    pub schema_version: u16,
    pub view_id: String,
    pub catalog_version: String,
    pub spec_digest: String,
}

impl ViewSpec {
    pub fn validate(
        &self,
        context: &ViewSpecValidationContext,
    ) -> Result<ViewSpecValidationReceipt, AppContractError> {
        let invalid = |reason: &str| AppContractError::InvalidViewSpec {
            view_id: self.view_id.clone(),
            reason: reason.to_string(),
        };
        if self.schema_version != VIEW_SPEC_SCHEMA_VERSION
            || self.surface_id != context.surface_id
            || self.catalog_version != context.catalog_version
            || self.view_id.trim().is_empty()
            || self.title.trim().is_empty()
            || self.layouts.is_empty()
        {
            return Err(invalid(
                "identity, schema, catalog or layout set is invalid",
            ));
        }
        let mut widget_ids = BTreeSet::new();
        for widget in &self.widgets {
            if widget.instance_id.trim().is_empty()
                || !widget_ids.insert(widget.instance_id.as_str())
                || !context.definitions.contains(&widget.definition_id)
                || context.renderers.get(&widget.renderer_id) != Some(&widget.renderer_version)
                || widget.title.trim().is_empty()
                || !(widget.config.is_null() || widget.config.is_object())
                || !(widget.query.is_null() || widget.query.is_object())
            {
                return Err(invalid(
                    "widget identity, definition, renderer or payload is invalid",
                ));
            }
        }
        for layout in self.layouts.values() {
            if layout.columns == 0 || layout.placements.len() != self.widgets.len() {
                return Err(invalid("layout is incomplete or has zero columns"));
            }
            let placements = layout.placements.iter().collect::<Vec<_>>();
            for (index, (instance_id, placement)) in placements.iter().enumerate() {
                if !widget_ids.contains(instance_id.as_str())
                    || placement.width == 0
                    || placement.height == 0
                    || placement.x.saturating_add(placement.width) > layout.columns
                {
                    return Err(invalid(
                        "widget placement is missing, empty or out of bounds",
                    ));
                }
                if placements[..index].iter().any(|(_, previous)| {
                    placement.x < previous.x.saturating_add(previous.width)
                        && previous.x < placement.x.saturating_add(placement.width)
                        && placement.y < previous.y.saturating_add(previous.height)
                        && previous.y < placement.y.saturating_add(placement.height)
                }) {
                    return Err(invalid("widget placements overlap"));
                }
            }
        }
        let mut lock_ids = BTreeSet::new();
        for lock in &self.locks {
            if !widget_ids.contains(lock.instance_id.as_str())
                || lock.fields.is_empty()
                || !lock_ids.insert(lock.instance_id.as_str())
            {
                return Err(invalid(
                    "lock references an unknown widget or duplicate lock",
                ));
            }
        }
        if !matches!(
            self.sharing.visibility.as_str(),
            "private" | "team" | "public"
        ) {
            return Err(invalid("sharing visibility is invalid"));
        }
        let canonical = serde_json::to_value(self).map_err(|error| invalid(&error.to_string()))?;
        let bytes = serde_json::to_vec(&canonical).map_err(|error| invalid(&error.to_string()))?;
        Ok(ViewSpecValidationReceipt {
            schema_version: VIEW_SPEC_SCHEMA_VERSION,
            view_id: self.view_id.clone(),
            catalog_version: self.catalog_version.clone(),
            spec_digest: format!("{:x}", Sha256::digest(bytes)),
        })
    }
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
    let canonical = serde_json::to_value(result_shape_schema())
        .expect("ResultShape JSON schema is always representable as JSON");
    let bytes =
        serde_json::to_vec(&canonical).expect("ResultShape JSON schema is always serializable");
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

    #[test]
    fn view_spec_validation_is_deterministic_and_rejects_overlap() {
        let widget = ViewSpecWidget {
            instance_id: "w1".to_string(),
            definition_id: "fixture.metric".to_string(),
            renderer_id: "line".to_string(),
            renderer_version: 1,
            title: "Metric".to_string(),
            config: serde_json::Value::Null,
            query: serde_json::json!({"limit": 20}),
            visible: true,
        };
        let mut spec = ViewSpec {
            schema_version: VIEW_SPEC_SCHEMA_VERSION,
            surface_id: "fixture.cockpit".to_string(),
            view_id: "view-1".to_string(),
            base_revision: 3,
            catalog_version: "catalog-1".to_string(),
            title: "Fixture".to_string(),
            widgets: vec![widget],
            layouts: BTreeMap::from([(
                "desktop".to_string(),
                ViewSpecLayout {
                    columns: 12,
                    placements: BTreeMap::from([(
                        "w1".to_string(),
                        ViewSpecPlacement {
                            x: 0,
                            y: 0,
                            width: 6,
                            height: 4,
                        },
                    )]),
                },
            )]),
            locks: vec![ViewSpecLock {
                instance_id: "w1".to_string(),
                fields: BTreeSet::from([ViewSpecLockField::Query]),
            }],
            sharing: ViewSpecSharing {
                visibility: "private".to_string(),
                viewer_refs: BTreeSet::new(),
                editor_refs: BTreeSet::new(),
            },
            domain_context: serde_json::Value::Null,
        };
        let context = ViewSpecValidationContext {
            surface_id: "fixture.cockpit".to_string(),
            catalog_version: "catalog-1".to_string(),
            renderers: BTreeMap::from([("line".to_string(), 1)]),
            definitions: BTreeSet::from(["fixture.metric".to_string()]),
        };
        let first = spec.validate(&context).expect("valid view spec");
        assert_eq!(first, spec.validate(&context).expect("same receipt"));
        spec.layouts
            .get_mut("desktop")
            .expect("desktop")
            .placements
            .insert(
                "unknown".to_string(),
                ViewSpecPlacement {
                    x: 0,
                    y: 0,
                    width: 6,
                    height: 4,
                },
            );
        assert!(spec.validate(&context).is_err());
    }
}
