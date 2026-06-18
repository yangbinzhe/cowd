use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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

#[cfg(test)]
mod tests {
    use super::*;
    use matrix::MatrixMetricDependencyInput;

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
