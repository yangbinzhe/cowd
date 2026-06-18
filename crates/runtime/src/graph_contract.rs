use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::matrix::MatrixMetricDependency;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CowdGraphNode {
    pub node_id: String,
    pub node_type: String,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CowdGraphEdge {
    pub edge_id: String,
    pub from: String,
    pub to: String,
    pub relation: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CowdGraphPath {
    pub path_id: String,
    pub path_type: String,
    #[serde(default)]
    pub nodes: Vec<CowdGraphNode>,
    #[serde(default)]
    pub edges: Vec<CowdGraphEdge>,
    #[serde(default)]
    pub structured_refs: Vec<String>,
    pub confidence: f32,
    pub created_at: DateTime<Utc>,
}

impl From<&MatrixMetricDependency> for CowdGraphPath {
    fn from(dependency: &MatrixMetricDependency) -> Self {
        let upstream = format!("metric:{}", dependency.upstream_metric_id);
        let downstream = format!("metric:{}", dependency.downstream_metric_id);
        Self {
            path_id: format!("graph-path:{}", dependency.dependency_id),
            path_type: "metric_dependency".to_string(),
            nodes: vec![
                CowdGraphNode {
                    node_id: upstream.clone(),
                    node_type: "metric".to_string(),
                    label: dependency.upstream_metric_id.clone(),
                },
                CowdGraphNode {
                    node_id: downstream.clone(),
                    node_type: "metric".to_string(),
                    label: dependency.downstream_metric_id.clone(),
                },
            ],
            edges: vec![CowdGraphEdge {
                edge_id: dependency.dependency_id.clone(),
                from: upstream,
                to: downstream,
                relation: dependency.dependency_type.clone(),
                evidence_refs: dependency
                    .required_fact_types
                    .iter()
                    .map(|fact_type| format!("structured-fact-type:{fact_type}"))
                    .collect(),
            }],
            structured_refs: dependency
                .required_fact_types
                .iter()
                .map(|fact_type| format!("structured-fact-type:{fact_type}"))
                .collect(),
            confidence: dependency.confidence,
            created_at: dependency.created_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matrix::MatrixMetricDependencyInput;

    #[test]
    fn metric_dependency_maps_to_graph_path_with_structured_refs() {
        let dependency = MatrixMetricDependency::from_input(MatrixMetricDependencyInput {
            dependency_id: Some("dep-1".to_string()),
            upstream_metric_id: "material_shortage_risk".to_string(),
            downstream_metric_id: "order_delivery_risk".to_string(),
            dependency_type: "drives".to_string(),
            entity_relation_type: None,
            required_fact_types: vec![
                "inventory_balance".to_string(),
                "supplier_commit".to_string(),
            ],
            transformation_ref: None,
            confidence: Some(0.82),
            notes: None,
        });

        let path = CowdGraphPath::from(&dependency);

        assert_eq!(path.path_type, "metric_dependency");
        assert_eq!(path.nodes.len(), 2);
        assert_eq!(path.edges[0].relation, "drives");
        assert!(path
            .structured_refs
            .contains(&"structured-fact-type:inventory_balance".to_string()));
        assert_eq!(path.confidence, 0.82);
    }
}
