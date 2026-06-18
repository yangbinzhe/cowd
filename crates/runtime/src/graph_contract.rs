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

    #[test]
    fn graph_path_preserves_structured_refs_without_matrix_dependency() {
        let path = CowdGraphPath {
            path_id: "dep-1".to_string(),
            path_type: "metric_dependency".to_string(),
            nodes: vec![
                CowdGraphNode {
                    node_id: "metric:material_shortage_risk".to_string(),
                    node_type: "metric".to_string(),
                    label: "material_shortage_risk".to_string(),
                },
                CowdGraphNode {
                    node_id: "metric:order_delivery_risk".to_string(),
                    node_type: "metric".to_string(),
                    label: "order_delivery_risk".to_string(),
                },
            ],
            edges: vec![CowdGraphEdge {
                edge_id: "dep-1".to_string(),
                from: "metric:material_shortage_risk".to_string(),
                to: "metric:order_delivery_risk".to_string(),
                relation: "drives".to_string(),
                evidence_refs: vec!["structured-fact-type:inventory_balance".to_string()],
            }],
            structured_refs: vec![
                "inventory_balance".to_string(),
                "supplier_commit".to_string(),
            ]
            .into_iter()
            .map(|fact_type| format!("structured-fact-type:{fact_type}"))
            .collect(),
            confidence: 0.82,
            created_at: DateTime::<Utc>::UNIX_EPOCH,
        };

        assert_eq!(path.path_type, "metric_dependency");
        assert_eq!(path.nodes.len(), 2);
        assert_eq!(path.edges[0].relation, "drives");
        assert!(path
            .structured_refs
            .contains(&"structured-fact-type:inventory_balance".to_string()));
        assert_eq!(path.confidence, 0.82);
    }
}
