use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::rewoo_plan::RewooEvidencePlan;
use crate::tool_dispatch::ToolRequest;

/// ReWOO contributes model intent and explicit dependencies only. Effect,
/// permission, resource and scheduling decisions are compiled later by the
/// sole `GovernedToolCompiler` against a pinned ToolHost catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolIntentGraph {
    pub graph_id: String,
    pub intents: Vec<ToolIntentNode>,
    pub dependencies: Vec<ToolIntentDependency>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolIntentNode {
    pub id: String,
    pub tool_name: String,
    pub input: Value,
    pub purpose: String,
    pub expected_output: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolIntentDependency {
    pub from: String,
    pub to: String,
    pub kind: ToolIntentDependencyKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolIntentDependencyKind {
    DataDependency,
    Ordering,
}

impl ToolIntentGraph {
    #[must_use]
    pub fn new(intents: Vec<ToolIntentNode>, dependencies: Vec<ToolIntentDependency>) -> Self {
        Self {
            graph_id: format!("tool-intents-{}", Uuid::new_v4()),
            intents,
            dependencies,
        }
    }

    #[must_use]
    pub fn to_tool_requests(&self) -> Vec<ToolRequest> {
        self.intents
            .iter()
            .map(|intent| ToolRequest {
                tool_use_id: intent.id.clone(),
                tool_name: intent.tool_name.clone(),
                input: serde_json::to_string(&intent.input).unwrap_or_else(|_| "{}".to_string()),
                depends_on: self
                    .dependencies
                    .iter()
                    .filter(|dependency| dependency.to == intent.id)
                    .map(|dependency| dependency.from.clone())
                    .collect(),
            })
            .collect()
    }
}

#[must_use]
pub fn tool_intents_from_rewoo(plan: &RewooEvidencePlan) -> ToolIntentGraph {
    let intents = plan
        .steps
        .iter()
        .map(|step| ToolIntentNode {
            id: step.id.clone(),
            tool_name: step.tool_name.clone(),
            input: step.input.clone(),
            purpose: step.purpose.clone(),
            expected_output: step.output_ref.clone(),
        })
        .collect::<Vec<_>>();
    let dependencies = plan
        .steps
        .iter()
        .flat_map(|step| {
            step.depends_on.iter().map(|from| ToolIntentDependency {
                from: from.clone(),
                to: step.id.clone(),
                kind: ToolIntentDependencyKind::DataDependency,
            })
        })
        .collect::<Vec<_>>();
    ToolIntentGraph::new(intents, dependencies)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intent_graph_preserves_dependencies_without_inventing_safety() {
        let graph = ToolIntentGraph::new(
            vec![
                ToolIntentNode {
                    id: "a".to_string(),
                    tool_name: "read_file".to_string(),
                    input: serde_json::json!({"path": "README.md"}),
                    purpose: "read".to_string(),
                    expected_output: "a".to_string(),
                },
                ToolIntentNode {
                    id: "b".to_string(),
                    tool_name: "grep_search".to_string(),
                    input: serde_json::json!({"pattern": "runtime"}),
                    purpose: "grep".to_string(),
                    expected_output: "b".to_string(),
                },
            ],
            vec![ToolIntentDependency {
                from: "a".to_string(),
                to: "b".to_string(),
                kind: ToolIntentDependencyKind::DataDependency,
            }],
        );
        assert_eq!(graph.to_tool_requests()[1].depends_on, vec!["a"]);
    }
}
